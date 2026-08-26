"""Skybrush Server integration for the fail-closed RTK guard."""

from __future__ import annotations

from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any

from flockwave.connections import create_connection
from flockwave.gps.formatting import format_gps_coordinate_as_nmea_gga_message
from flockwave.gps.vectors import GPSCoordinate
from flockwave.server.ext.base import Extension
from trio import open_nursery, sleep
from trio_util import periodic

from .config import GuardConfig, SourceConfig
from .decoder import RTCMObservationDecoder
from .model import GuardStatus
from .recorder import GuardRecorder
from .state_machine import RTKGuardEngine


class RTKGuardExtension(Extension):
    """Continuously qualifies a backup RTCM source and controls failover."""

    def __init__(self) -> None:
        super().__init__()
        self._config: GuardConfig | None = None
        self._engine: RTKGuardEngine | None = None
        self._status: GuardStatus | None = None
        self._rtk_api: Any = None
        self._get_location: Any = None
        self._recorder: GuardRecorder | None = None
        self._active_decoder = RTCMObservationDecoder()
        self._passive_connections: dict[str, Any] = {}
        self._suspended_passive_roles: set[str] = set()
        self._switch_requested_at: float | None = None
        self._switch_timed_out = False
        self._last_forwarded_backup_packet_at: float | None = None
        self._last_logged_status: tuple[Any, ...] | None = None

    def configure(self, configuration: dict[str, Any]) -> None:
        self._config = GuardConfig.from_json(configuration)
        self._engine = RTKGuardEngine(self._config)

    def exports(self) -> dict[str, Any]:
        return {"get_status": self.get_status}

    def get_status(self) -> dict[str, Any] | None:
        return self._status.json if self._status else None

    def handle_X_RTK_GUARD_STAT(self, message, sender, hub):
        return hub.create_response_or_notification(
            body=self.get_status() or {"available": False}, in_response_to=message
        )

    async def run(self, app, configuration, logger) -> None:
        assert self._config is not None
        assert self._engine is not None
        if not self._config.enabled:
            logger.info("RTK guard is disabled")
            await sleep_forever()
            return

        self._rtk_api = app.import_api("rtk")
        self._get_location = app.import_api("location").get_location
        recording_directory = self._config.recording_directory
        if not recording_directory:
            session = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")
            recording_directory = str(
                Path(self.get_data_dir()) / "recordings" / session
            )
        self._recorder = GuardRecorder(recording_directory)
        logger.info(f"Recording RTK guard inputs and decisions in {recording_directory}")

        with ExitStack() as stack:
            if self._recorder:
                stack.callback(self._recorder.close)
            stack.enter_context(
                app.message_hub.use_message_handlers(
                    {"X-RTK-GUARD-STAT": self.handle_X_RTK_GUARD_STAT},
                )
            )
            rtk_packet_signal = app.import_api("signals").get("rtk:packet")
            stack.enter_context(rtk_packet_signal.connected_to(self._on_rtk_packet))
            async with open_nursery() as nursery:
                nursery.start_soon(self._monitor_source, "primary", self._config.primary)
                nursery.start_soon(self._monitor_source, "backup", self._config.backup)
                nursery.start_soon(self._run_decision_loop)

    def _resolve_source(self, config: SourceConfig) -> str:
        if config.source:
            return config.source
        preset = self._rtk_api.get_preset(config.preset_id)
        if preset is None:
            raise RuntimeError(f"No RTK preset named {config.preset_id!r}")
        if not preset.sources:
            raise RuntimeError(f"RTK preset {config.preset_id!r} has no source")
        return preset.sources[0]

    async def _monitor_source(self, role: str, config: SourceConfig) -> None:
        assert self._engine is not None
        decoder = RTCMObservationDecoder()
        while True:
            try:
                current = self._rtk_api.get_current_preset()
                if (
                    role in self._suspended_passive_roles
                    or (
                        current is not None
                        and current.id == config.preset_id
                    )
                ):
                    await sleep(0.2)
                    continue
                source = self._resolve_source(config)
                connection = create_connection(source)
                self._passive_connections[role] = connection
                try:
                    async with connection:
                        if "NTRIPConnection" in type(connection).__name__:
                            async with open_nursery() as nursery:
                                nursery.start_soon(
                                    self._send_vrs_positions, connection
                                )
                                await self._read_passive_source(
                                    role, config, connection, decoder
                                )
                                nursery.cancel_scope.cancel()
                        else:
                            await self._read_passive_source(
                                role, config, connection, decoder
                            )
                finally:
                    if self._passive_connections.get(role) is connection:
                        self._passive_connections.pop(role, None)
            except Exception as exc:
                if self.log and role not in self._suspended_passive_roles:
                    self.log.warning(
                        f"RTK guard lost its passive {role} monitoring connection: {exc}"
                    )
                await sleep(1)

    async def _read_passive_source(
        self,
        role: str,
        config: SourceConfig,
        connection,
        decoder: RTCMObservationDecoder,
    ) -> None:
        while True:
            data = await connection.read(65536)
            current = self._rtk_api.get_current_preset()
            if (
                role in self._suspended_passive_roles
                or (current is not None and current.id == config.preset_id)
            ):
                return
            if not data:
                await sleep(0.05)
                continue
            timestamp = monotonic()
            if self._recorder:
                self._recorder.record_packet(role, data, timestamp)
            self._feed(role, decoder.feed(data), timestamp)

    async def _send_vrs_positions(self, connection) -> None:
        """Keeps a passive NTRIP VRS mountpoint supplied with rover positions."""
        if not callable(self._get_location):
            return
        async for _ in periodic(5):
            location = self._get_location()
            if location is None or location.position is None:
                continue
            position = location.position
            message = format_gps_coordinate_as_nmea_gga_message(
                GPSCoordinate(position.lat, position.lon, amsl=position.amsl or 0.0)
            )
            await connection.write(message.encode("ascii"))

    async def _run_decision_loop(self) -> None:
        assert self._config is not None
        assert self._engine is not None
        async for _ in periodic(0.2):
            now = monotonic()
            current = self._rtk_api.get_current_preset()
            active_id = current.id if current is not None else None
            backup_confirmed = (
                self._switch_requested_at is None
                or (
                    self._last_forwarded_backup_packet_at is not None
                    and self._last_forwarded_backup_packet_at
                    > self._switch_requested_at
                )
            )
            status = self._engine.tick(
                now, active_id, backup_active_confirmed=backup_confirmed
            )
            self._status = status
            self._record_decision_if_changed(status)

            if active_id == self._config.backup.preset_id and backup_confirmed:
                self._switch_requested_at = None
                self._switch_timed_out = False
                self._suspended_passive_roles.discard("backup")
                continue
            if self._switch_requested_at is not None:
                if (
                    not self._switch_timed_out
                    and now - self._switch_requested_at
                    > self._config.switch_confirmation_timeout
                ):
                    self._engine.mark_switch_failed()
                    self._switch_timed_out = True
                    self._suspended_passive_roles.discard("backup")
                    if self.log:
                        self.log.error("RTK guard source switch was not confirmed")
                continue
            if not status.switch_allowed or not status.automatic:
                continue

            # This single decision task is the switch lock: status and active
            # source are checked again immediately before requesting the switch.
            rechecked = self._engine.tick(monotonic(), active_id)
            if not rechecked.switch_allowed or active_id != self._config.primary.preset_id:
                continue
            self._engine.mark_switching()
            self._switch_requested_at = monotonic()
            self._switch_timed_out = False
            try:
                await self._suspend_passive_source("backup")
                await self._rtk_api.request_source_switch(
                    self._config.backup.preset_id
                )
                if self.log:
                    self.log.warning(
                        "RTK guard requested failover to the qualified backup source"
                    )
            except Exception:
                self._engine.mark_switch_failed()
                self._switch_timed_out = True
                self._suspended_passive_roles.discard("backup")
                if self.log:
                    self.log.exception("RTK guard failed to request source switch")

    def _on_rtk_packet(self, sender=None, packet=None, **kwargs) -> None:
        """Consumes the active RTK stream without opening a duplicate connection."""
        if self._config is None or self._engine is None or not isinstance(packet, bytes):
            return
        current = self._rtk_api.get_current_preset()
        if current is None:
            return
        if current.id == self._config.primary.preset_id:
            role = "primary"
        elif current.id == self._config.backup.preset_id:
            role = "backup"
            self._last_forwarded_backup_packet_at = monotonic()
        else:
            return

        timestamp = monotonic()
        if self._recorder:
            self._recorder.record_packet(role, packet, timestamp)
        self._feed(role, self._active_decoder.feed(packet), timestamp)

    def _feed(self, role: str, packets, timestamp: float) -> None:
        assert self._engine is not None
        feed = (
            self._engine.feed_primary
            if role == "primary"
            else self._engine.feed_backup
        )
        for packet in packets:
            feed(packet, timestamp)

    async def _suspend_passive_source(self, role: str) -> None:
        """Releases an inactive source before RTK makes it the active source."""
        self._suspended_passive_roles.add(role)
        connection = self._passive_connections.get(role)
        if connection is not None:
            await connection.close()

    def _record_decision_if_changed(self, status: GuardStatus) -> None:
        key = (
            status.primary_state,
            status.backup_state,
            status.pair.state,
            status.decision,
            status.decision_reason,
            status.failover_state,
        )
        if key == self._last_logged_status:
            return
        self._last_logged_status = key
        if self._recorder:
            self._recorder.record_status(status)
        if self.log:
            action = "WOULD_SWITCH" if status.switch_allowed else "SWITCH_INHIBITED"
            self.log.info(
                f"RTK guard: {action}; reason={status.decision_reason}; "
                f"primary={status.primary_state.value}; "
                f"backup={status.backup_state.value}; pair={status.pair.state.value}"
            )


async def sleep_forever() -> None:
    while True:
        await sleep(3600)


construct = RTKGuardExtension
dependencies = ("location", "rtk", "signals")
description = "Fail-closed monitoring and automatic failover for redundant RTK bases"


def get_schema() -> dict[str, Any]:
    source = {
        "type": "object",
        "properties": {
            "preset_id": {"type": "string"},
            "source": {"type": "string"},
            "expected_ecef": {
                "type": "array",
                "minItems": 3,
                "maxItems": 3,
                "items": {"type": "number"},
            },
        },
        "required": ["preset_id"],
    }
    return {
        "type": "object",
        "properties": {
            "enabled": {"type": "boolean", "default": True},
            "mode": {"type": "string", "enum": ["dry_run", "active"], "default": "dry_run"},
            "primary": source,
            "backup": source,
            "health": {
                "type": "object",
                "properties": {
                    "degraded_msm_age": {"type": "number", "default": 1.5},
                    "max_msm_age": {"type": "number", "default": 2.5},
                    "max_position_age": {"type": "number", "default": 30},
                    "maximum_position_error": {"type": "number", "default": 0.05},
                    "maximum_position_jump": {"type": "number", "default": 0.05},
                    "minimum_satellites": {"type": "integer", "default": 10},
                    "minimum_common_satellites": {"type": "integer", "default": 8},
                    "minimum_common_signals": {"type": "integer", "default": 8},
                    "required_constellations": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": ["GPS", "GLONASS", "GALILEO", "BEIDOU"],
                        },
                        "default": ["GPS", "GALILEO"],
                    },
                    "primary_failure_hold": {"type": "number", "default": 2},
                    "backup_ready_hold": {"type": "number", "default": 10},
                    "pair_recent_window": {"type": "number", "default": 15},
                    "epoch_tolerance": {"type": "number", "default": 0.05},
                    "maximum_phase_outlier_ratio": {
                        "type": "number",
                        "default": 0.25,
                    },
                    "phase_mad_multiplier": {"type": "number", "default": 6},
                    "phase_absolute_floor": {"type": "number", "default": 0.03},
                    "pair_disagreement_hold": {"type": "number", "default": 2},
                    "mass_cycle_slip_count": {"type": "integer", "default": 4},
                    "cycle_slip_window": {"type": "number", "default": 2},
                    "minimum_cnr": {"type": "number", "default": 25},
                    "cnr_collapse_delta": {"type": "number", "default": 8},
                    "enable_phase_comparison": {"type": "boolean", "default": True},
                },
                "additionalProperties": False,
            },
            "switching": {
                "type": "object",
                "properties": {
                    "enabled": {"type": "boolean", "default": False},
                    "allow_auto_failback": {
                        "type": "boolean",
                        "enum": [False],
                        "default": False,
                    },
                    "confirmation_timeout": {
                        "type": "number",
                        "minimum": 0.1,
                        "default": 10,
                    },
                },
            },
            "recording_directory": {"type": "string"},
        },
        "required": ["primary", "backup"],
    }
