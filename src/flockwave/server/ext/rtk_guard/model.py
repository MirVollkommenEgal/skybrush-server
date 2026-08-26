"""Domain model for the RTK guard.

The types in this module deliberately have no Skybrush or connection-layer
dependencies so recorded streams can be replayed through the same engine.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


class PrimaryState(str, Enum):
    GOOD = "GOOD"
    DEGRADED = "DEGRADED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class BackupState(str, Enum):
    READY = "READY"
    DEGRADED = "DEGRADED"
    NOT_READY = "NOT_READY"
    UNKNOWN = "UNKNOWN"


class PairState(str, Enum):
    CONSISTENT = "CONSISTENT"
    DISAGREE = "DISAGREE"
    NOT_COMPARABLE = "NOT_COMPARABLE"


class SwitchDecision(str, Enum):
    ALLOWED = "ALLOWED"
    INHIBITED = "INHIBITED"


class FailoverState(str, Enum):
    PRIMARY_ACTIVE = "PRIMARY_ACTIVE"
    PRIMARY_SUSPECT = "PRIMARY_SUSPECT"
    PRIMARY_FAILED = "PRIMARY_FAILED"
    SWITCH_INHIBITED = "SWITCH_INHIBITED"
    SWITCH_PENDING = "SWITCH_PENDING"
    SWITCHING = "SWITCHING"
    BACKUP_ACTIVE = "BACKUP_ACTIVE"
    SWITCH_FAILED = "SWITCH_FAILED"


class FaultKind(str, Enum):
    STREAM_LOSS = "STREAM_LOSS"
    MSM_STALE = "MSM_STALE"
    MSM_TIMEOUT = "MSM_TIMEOUT"
    POSITION_JUMP = "POSITION_JUMP"
    POSITION_MISMATCH = "POSITION_MISMATCH"
    POSITION_MESSAGE_MISSING = "POSITION_MESSAGE_MISSING"
    STATION_ID_CHANGE = "STATION_ID_CHANGE"
    MASS_CYCLE_SLIP = "MASS_CYCLE_SLIP"
    CNR_COLLAPSE = "CNR_COLLAPSE"
    SATELLITE_COLLAPSE = "SATELLITE_COLLAPSE"
    PHASE_INSTABILITY = "PHASE_INSTABILITY"
    UNKNOWN_DISAGREEMENT = "UNKNOWN_DISAGREEMENT"


@dataclass(frozen=True, order=True)
class SatKey:
    constellation: str
    satellite_id: int
    signal_id: int


@dataclass
class SatelliteObservation:
    epoch: float
    pseudorange: Optional[float]
    phase_range: Optional[float]
    cnr: Optional[float]
    lock_time: int
    half_cycle: bool


@dataclass
class DecodedRTCM:
    message_type: int
    station_id: Optional[int] = None
    constellation: Optional[str] = None
    epoch: Optional[float] = None
    reported_ecef: Optional[tuple[float, float, float]] = None
    observations: dict[SatKey, SatelliteObservation] = field(default_factory=dict)


@dataclass
class BaseState:
    source_id: str
    expected_ecef: Optional[tuple[float, float, float]] = None
    reference_ecef: Optional[tuple[float, float, float]] = None
    station_id: Optional[int] = None
    reported_ecef: Optional[tuple[float, float, float]] = None
    last_packet_monotonic: Optional[float] = None
    last_position_monotonic: Optional[float] = None
    last_msm_by_constellation: dict[str, float] = field(default_factory=dict)
    msm_epochs: dict[str, float] = field(default_factory=dict)
    satellites: dict[SatKey, SatelliteObservation] = field(default_factory=dict)
    previous_locks: dict[SatKey, int] = field(default_factory=dict)
    previous_half_cycles: dict[SatKey, bool] = field(default_factory=dict)
    cycle_slips_at: list[float] = field(default_factory=list)
    station_id_changed: bool = False
    position_jumped: bool = False
    position_jump_distance: float = 0.0
    cnr_history: dict[str, list[float]] = field(default_factory=dict)
    bad_since: Optional[float] = None
    good_since: Optional[float] = None


@dataclass(frozen=True)
class SourceHealth:
    healthy: bool
    failed: bool
    attributable: bool
    reasons: tuple[str, ...]
    msm_age: Optional[float]
    satellite_count: int
    median_cnr: Optional[float]


@dataclass(frozen=True)
class PairHealth:
    state: PairState
    reasons: tuple[str, ...]
    common_satellites: int
    common_signals: int
    overlap: float
    phase_outlier_ratio: Optional[float]


@dataclass(frozen=True)
class GuardStatus:
    timestamp: float
    active: Optional[str]
    primary_state: PrimaryState
    backup_state: BackupState
    pair: PairHealth
    decision: SwitchDecision
    decision_reason: str
    failover_state: FailoverState
    primary_health: SourceHealth
    backup_health: SourceHealth
    pair_recent: bool
    automatic: bool
    mode: str

    @property
    def switch_allowed(self) -> bool:
        return self.decision is SwitchDecision.ALLOWED

    @property
    def json(self) -> dict:
        def source(state: Enum, health: SourceHealth) -> dict:
            return {
                "state": state.value,
                "reasons": list(health.reasons),
                "msmAge": health.msm_age,
                "satellites": health.satellite_count,
                "medianCnr": health.median_cnr,
            }

        return {
            "timestamp": self.timestamp,
            "active": self.active,
            "primary": source(self.primary_state, self.primary_health),
            "backup": source(self.backup_state, self.backup_health),
            "pair": {
                "state": self.pair.state.value,
                "reasons": list(self.pair.reasons),
                "commonSatellites": self.pair.common_satellites,
                "commonSignals": self.pair.common_signals,
                "overlap": self.pair.overlap,
                "phaseOutlierRatio": self.pair.phase_outlier_ratio,
                "recent": self.pair_recent,
            },
            "switch": {
                "decision": self.decision.value,
                "allowed": self.switch_allowed,
                "automatic": self.automatic,
                "armed": (
                    self.automatic
                    and self.backup_state is BackupState.READY
                    and (
                        self.pair.state is PairState.CONSISTENT
                        or (
                            self.pair.state is PairState.NOT_COMPARABLE
                            and self.pair_recent
                        )
                    )
                ),
                "reason": self.decision_reason,
                "state": self.failover_state.value,
                "mode": self.mode,
            },
        }
