"""Fail-closed RTK guard engine and switch state machine."""

from __future__ import annotations

from .config import GuardConfig
from .model import (
    BackupState,
    BaseState,
    DecodedRTCM,
    FailoverState,
    GuardStatus,
    PairHealth,
    PairState,
    PrimaryState,
    SwitchDecision,
)
from .pair_comparator import PairComparator
from .source_health import evaluate_source, update_base


class RTKGuardEngine:
    """Pure decision engine suitable for live input and deterministic replay."""

    def __init__(self, config: GuardConfig) -> None:
        self.config = config
        self.primary = BaseState(
            config.primary.preset_id, expected_ecef=config.primary.expected_ecef
        )
        self.backup = BaseState(
            config.backup.preset_id, expected_ecef=config.backup.expected_ecef
        )
        self._comparator = PairComparator(config.health)
        self._last_pair_consistent_at: float | None = None
        self._switching = False
        self._switch_failed = False

    def feed_primary(self, packet: DecodedRTCM, timestamp: float) -> None:
        update_base(self.primary, packet, timestamp)

    def feed_backup(self, packet: DecodedRTCM, timestamp: float) -> None:
        update_base(self.backup, packet, timestamp)

    def mark_switching(self) -> None:
        self._switching = True
        self._switch_failed = False

    def mark_switch_failed(self) -> None:
        self._switching = False
        self._switch_failed = True

    def tick(
        self,
        timestamp: float,
        active_source: str | None,
        *,
        backup_active_confirmed: bool = True,
    ) -> GuardStatus:
        health_config = self.config.health
        primary_health = evaluate_source(self.primary, timestamp, health_config)
        backup_health = evaluate_source(self.backup, timestamp, health_config)

        if primary_health.healthy:
            self.primary.bad_since = None
            primary_state = PrimaryState.GOOD
        elif self.primary.last_packet_monotonic is None:
            primary_state = PrimaryState.UNKNOWN
        else:
            if self.primary.bad_since is None:
                self.primary.bad_since = timestamp
            bad_duration = timestamp - self.primary.bad_since
            primary_state = (
                PrimaryState.FAILED
                if primary_health.failed
                or bad_duration >= health_config.primary_failure_hold
                else PrimaryState.DEGRADED
            )

        if backup_health.healthy:
            if self.backup.good_since is None:
                self.backup.good_since = timestamp
            ready_duration = timestamp - self.backup.good_since
            backup_state = (
                BackupState.READY
                if ready_duration >= health_config.backup_ready_hold
                else BackupState.DEGRADED
            )
        elif self.backup.last_packet_monotonic is None:
            self.backup.good_since = None
            backup_state = BackupState.UNKNOWN
        else:
            self.backup.good_since = None
            backup_state = BackupState.NOT_READY

        pair = self._comparator.compare(self.primary, self.backup, timestamp)
        freshness_faults = {"STREAM_LOSS", "MSM_TIMEOUT"}
        if freshness_faults.intersection(primary_health.reasons) or freshness_faults.intersection(
            backup_health.reasons
        ):
            pair = PairHealth(
                PairState.NOT_COMPARABLE,
                ("SOURCE_NOT_FRESH",),
                pair.common_satellites,
                pair.common_signals,
                pair.overlap,
                pair.phase_outlier_ratio,
            )
        if pair.state is PairState.CONSISTENT:
            self._last_pair_consistent_at = timestamp
        pair_recent = (
            self._last_pair_consistent_at is not None
            and timestamp - self._last_pair_consistent_at
            <= health_config.pair_recent_window
        )

        decision = SwitchDecision.INHIBITED
        reason = "PRIMARY_NOT_ACTIVE"
        if active_source == self.config.backup.preset_id:
            reason = "BACKUP_ALREADY_ACTIVE"
        elif active_source == self.config.primary.preset_id:
            if primary_state is not PrimaryState.FAILED:
                reason = "PRIMARY_NOT_FAILED"
            elif backup_state is not BackupState.READY:
                reason = "BACKUP_NOT_READY"
            elif pair.state is PairState.DISAGREE and not primary_health.attributable:
                reason = "AMBIGUOUS_BASE_DISAGREEMENT"
            elif pair.state is not PairState.CONSISTENT and not pair_recent:
                reason = "NO_RECENT_PAIR_QUALIFICATION"
            else:
                decision = SwitchDecision.ALLOWED
                reason = "PRIMARY_FAILED_BACKUP_QUALIFIED"

        if (
            active_source == self.config.backup.preset_id
            and backup_active_confirmed
        ):
            self._switching = False
            failover_state = FailoverState.BACKUP_ACTIVE
        elif self._switch_failed:
            failover_state = FailoverState.SWITCH_FAILED
        elif self._switching:
            failover_state = FailoverState.SWITCHING
        elif decision is SwitchDecision.ALLOWED:
            failover_state = FailoverState.SWITCH_PENDING
        elif primary_state is PrimaryState.FAILED:
            failover_state = FailoverState.SWITCH_INHIBITED
        elif primary_state is PrimaryState.DEGRADED:
            failover_state = FailoverState.PRIMARY_SUSPECT
        else:
            failover_state = FailoverState.PRIMARY_ACTIVE

        return GuardStatus(
            timestamp=timestamp,
            active=active_source,
            primary_state=primary_state,
            backup_state=backup_state,
            pair=pair,
            decision=decision,
            decision_reason=reason,
            failover_state=failover_state,
            primary_health=primary_health,
            backup_health=backup_health,
            pair_recent=pair_recent,
            automatic=(
                self.config.mode == "active" and self.config.switching_enabled
            ),
            mode=self.config.mode,
        )
