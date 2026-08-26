"""Configuration parsing for the RTK guard."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class SourceConfig:
    preset_id: str
    expected_ecef: Optional[tuple[float, float, float]] = None
    source: Optional[str] = None

    @classmethod
    def from_json(cls, value: dict[str, Any], name: str) -> "SourceConfig":
        preset_id = str(value.get("preset_id", name))
        position = value.get("expected_ecef")
        expected = None
        if position is not None:
            if not isinstance(position, (list, tuple)) or len(position) != 3:
                raise ValueError(f"{name}.expected_ecef must contain three numbers")
            expected = tuple(float(item) for item in position)
        source = value.get("source")
        return cls(preset_id, expected, str(source) if source else None)


@dataclass(frozen=True)
class HealthConfig:
    degraded_msm_age: float = 1.5
    max_msm_age: float = 2.5
    max_position_age: float = 30.0
    maximum_position_error: float = 0.05
    maximum_position_jump: float = 0.05
    minimum_satellites: int = 10
    minimum_common_satellites: int = 8
    minimum_common_signals: int = 8
    required_constellations: frozenset[str] = field(
        default_factory=lambda: frozenset(("GPS", "GALILEO"))
    )
    primary_failure_hold: float = 2.0
    backup_ready_hold: float = 10.0
    pair_recent_window: float = 15.0
    epoch_tolerance: float = 0.05
    maximum_phase_outlier_ratio: float = 0.25
    phase_mad_multiplier: float = 6.0
    phase_absolute_floor: float = 0.03
    pair_disagreement_hold: float = 2.0
    mass_cycle_slip_count: int = 4
    cycle_slip_window: float = 2.0
    minimum_cnr: float = 25.0
    cnr_collapse_delta: float = 8.0
    enable_phase_comparison: bool = True

    @classmethod
    def from_json(cls, value: dict[str, Any]) -> "HealthConfig":
        names = {item.name for item in cls.__dataclass_fields__.values()}
        kwargs = {key: item for key, item in value.items() if key in names}
        if "required_constellations" in kwargs:
            kwargs["required_constellations"] = frozenset(
                str(item).upper() for item in kwargs["required_constellations"]
            )
        return cls(**kwargs)


@dataclass(frozen=True)
class GuardConfig:
    primary: SourceConfig
    backup: SourceConfig
    health: HealthConfig = field(default_factory=HealthConfig)
    enabled: bool = True
    mode: str = "dry_run"
    switching_enabled: bool = False
    allow_auto_failback: bool = False
    switch_confirmation_timeout: float = 10.0
    recording_directory: Optional[str] = None

    @classmethod
    def from_json(cls, value: dict[str, Any]) -> "GuardConfig":
        mode = str(value.get("mode", "dry_run"))
        if mode not in ("dry_run", "active"):
            raise ValueError("rtk_guard.mode must be 'dry_run' or 'active'")
        switching = value.get("switching") or {}
        if switching.get("allow_auto_failback", False):
            raise ValueError("rtk_guard does not support automatic failback")
        return cls(
            primary=SourceConfig.from_json(value.get("primary") or {}, "primary"),
            backup=SourceConfig.from_json(value.get("backup") or {}, "backup"),
            health=HealthConfig.from_json(value.get("health") or {}),
            enabled=bool(value.get("enabled", True)),
            mode=mode,
            switching_enabled=bool(switching.get("enabled", False)),
            allow_auto_failback=False,
            switch_confirmation_timeout=float(
                switching.get("confirmation_timeout", 10.0)
            ),
            recording_directory=value.get("recording_directory"),
        )
