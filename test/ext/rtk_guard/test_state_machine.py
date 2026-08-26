from flockwave.server.ext.rtk_guard.config import GuardConfig, HealthConfig, SourceConfig
from flockwave.server.ext.rtk_guard.model import (
    BaseState,
    DecodedRTCM,
    PairState,
    SatKey,
    SatelliteObservation,
    SwitchDecision,
)
from flockwave.server.ext.rtk_guard.state_machine import RTKGuardEngine
from flockwave.server.ext.rtk_guard.source_health import evaluate_source, update_base


def make_config(**health_overrides):
    values = {
        "required_constellations": frozenset(("GPS",)),
        "minimum_satellites": 8,
        "minimum_common_satellites": 8,
        "minimum_common_signals": 8,
        "backup_ready_hold": 0,
        "primary_failure_hold": 0,
    }
    values.update(health_overrides)
    return GuardConfig(
        primary=SourceConfig("primary"),
        backup=SourceConfig("backup"),
        health=HealthConfig(**values),
        mode="active",
        switching_enabled=True,
    )


def msm(epoch=100.0, phase_changes=None, lock=10):
    phase_changes = phase_changes or {}
    observations = {}
    for satellite_id in range(1, 13):
        key = SatKey("GPS", satellite_id, 1)
        observations[key] = SatelliteObservation(
            epoch=epoch,
            pseudorange=20_000_000 + satellite_id,
            phase_range=20_000_000 + satellite_id + phase_changes.get(satellite_id, 0),
            cnr=45,
            lock_time=lock,
            half_cycle=False,
        )
    return DecodedRTCM(
        message_type=1077,
        station_id=1,
        constellation="GPS",
        epoch=epoch,
        observations=observations,
    )


def test_stream_loss_switches_only_to_a_qualified_backup():
    engine = RTKGuardEngine(make_config())
    engine.feed_primary(msm(), 0)
    engine.feed_backup(msm(), 0)
    initial = engine.tick(0, "primary")
    assert initial.pair.state is PairState.CONSISTENT

    engine.feed_backup(msm(epoch=103), 3)
    failed = engine.tick(3, "primary")
    assert failed.decision is SwitchDecision.ALLOWED
    assert failed.decision_reason == "PRIMARY_FAILED_BACKUP_QUALIFIED"


def test_pair_disagreement_never_causes_primary_failure_by_itself():
    engine = RTKGuardEngine(make_config(maximum_phase_outlier_ratio=0.25))
    engine.feed_primary(msm(), 0)
    engine.feed_backup(msm(), 0)
    engine.tick(0, "primary")
    engine.feed_primary(msm(epoch=101), 1)
    engine.feed_backup(
        msm(epoch=101, phase_changes=dict.fromkeys(range(1, 7), 0.2)),
        1,
    )
    status = engine.tick(1, "primary")
    assert status.pair.state is PairState.DISAGREE
    assert status.decision is SwitchDecision.INHIBITED
    assert status.decision_reason == "PRIMARY_NOT_FAILED"


def test_backup_fault_inhibits_failover():
    engine = RTKGuardEngine(make_config())
    engine.feed_primary(msm(), 0)
    engine.feed_backup(msm(), 0)
    engine.tick(0, "primary")
    engine.feed_backup(
        DecodedRTCM(message_type=1005, station_id=2, reported_ecef=(1, 2, 3)),
        1,
    )
    engine.feed_backup(
        DecodedRTCM(message_type=1005, station_id=3, reported_ecef=(2, 2, 3)),
        1.1,
    )
    status = engine.tick(3, "primary")
    assert status.decision is SwitchDecision.INHIBITED
    assert status.decision_reason == "BACKUP_NOT_READY"


def test_stream_loss_without_recent_pair_qualification_fails_closed():
    engine = RTKGuardEngine(make_config())
    engine.feed_primary(msm(), 0)
    engine.feed_backup(msm(epoch=101), 0)
    engine.tick(0, "primary")
    engine.feed_backup(msm(epoch=104), 3)
    status = engine.tick(3, "primary")
    assert status.decision is SwitchDecision.INHIBITED
    assert status.decision_reason == "NO_RECENT_PAIR_QUALIFICATION"


def test_switch_is_not_confirmed_before_backup_data_is_forwarded():
    engine = RTKGuardEngine(make_config())
    engine.mark_switching()
    status = engine.tick(1, "backup", backup_active_confirmed=False)
    assert status.failover_state.value == "SWITCHING"
    confirmed = engine.tick(1.1, "backup", backup_active_confirmed=True)
    assert confirmed.failover_state.value == "BACKUP_ACTIVE"


def test_unknown_ntrip_position_is_learned_and_cumulative_drift_is_detected():
    config = HealthConfig(
        required_constellations=frozenset(),
        minimum_satellites=0,
        maximum_position_error=0.05,
        maximum_position_jump=0.05,
    )
    state = BaseState("ntrip")
    update_base(
        state,
        DecodedRTCM(message_type=1005, station_id=1, reported_ecef=(1, 2, 3)),
        0,
    )
    assert state.reference_ecef == (1, 2, 3)

    update_base(
        state,
        DecodedRTCM(
            message_type=1005,
            station_id=1,
            reported_ecef=(1.03, 2, 3),
        ),
        0.5,
    )
    update_base(
        state,
        DecodedRTCM(
            message_type=1005,
            station_id=1,
            reported_ecef=(1.06, 2, 3),
        ),
        1,
    )
    health = evaluate_source(state, 1, config)
    assert "POSITION_MISMATCH" in health.reasons
    assert "POSITION_JUMP" not in health.reasons
