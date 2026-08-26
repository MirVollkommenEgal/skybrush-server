"""Health evaluation for one RTK base, independent of the other base."""

from __future__ import annotations

from math import dist
from statistics import median

from .config import HealthConfig
from .model import BaseState, DecodedRTCM, FaultKind, SourceHealth


def update_base(state: BaseState, packet: DecodedRTCM, now: float) -> None:
    state.last_packet_monotonic = now
    if packet.station_id is not None:
        if state.station_id is not None and packet.station_id != state.station_id:
            state.station_id_changed = True
        state.station_id = packet.station_id

    if packet.reported_ecef is not None:
        if state.reference_ecef is None:
            # NTRIP users often do not know the surveyed ECEF coordinate in
            # advance. Learn it from the first valid 1005/1006 packet and use
            # it as a session baseline when no configured coordinate exists.
            state.reference_ecef = packet.reported_ecef
        if state.reported_ecef is not None:
            state.position_jump_distance = dist(
                state.reported_ecef, packet.reported_ecef
            )
            state.position_jumped = True
        state.reported_ecef = packet.reported_ecef
        state.last_position_monotonic = now

    if packet.constellation and packet.epoch is not None:
        state.last_msm_by_constellation[packet.constellation] = now
        state.msm_epochs[packet.constellation] = packet.epoch
        for key in tuple(state.satellites):
            if key.constellation == packet.constellation:
                del state.satellites[key]

        cnrs: list[float] = []
        for key, observation in packet.observations.items():
            previous = state.previous_locks.get(key)
            previous_half_cycle = state.previous_half_cycles.get(key)
            if (previous is not None and observation.lock_time < previous) or (
                previous_half_cycle is not None
                and observation.half_cycle != previous_half_cycle
            ):
                state.cycle_slips_at.append(now)
            state.previous_locks[key] = observation.lock_time
            state.previous_half_cycles[key] = observation.half_cycle
            state.satellites[key] = observation
            if observation.cnr is not None:
                cnrs.append(observation.cnr)
        if cnrs:
            history = state.cnr_history.setdefault(packet.constellation, [])
            history.append(median(cnrs))
            del history[:-120]


def evaluate_source(state: BaseState, now: float, config: HealthConfig) -> SourceHealth:
    reasons: list[str] = []
    attributable = False

    packet_age = (
        now - state.last_packet_monotonic
        if state.last_packet_monotonic is not None
        else None
    )
    if packet_age is not None and packet_age > config.max_msm_age:
        reasons.append(FaultKind.STREAM_LOSS.value)
        attributable = True

    msm_ages = {
        constellation: now - state.last_msm_by_constellation[constellation]
        for constellation in config.required_constellations
        if constellation in state.last_msm_by_constellation
    }
    missing_constellations = config.required_constellations - msm_ages.keys()
    msm_age = max(msm_ages.values(), default=None)
    if missing_constellations or (
        msm_age is not None and msm_age > config.max_msm_age
    ):
        reasons.append(FaultKind.MSM_TIMEOUT.value)
        attributable = attributable or bool(state.last_msm_by_constellation)
    elif msm_age is not None and msm_age > config.degraded_msm_age:
        reasons.append(FaultKind.MSM_STALE.value)

    if state.last_position_monotonic is None:
        if state.last_msm_by_constellation and (
            now - min(state.last_msm_by_constellation.values())
            > config.max_position_age
        ):
            reasons.append(FaultKind.POSITION_MESSAGE_MISSING.value)
            attributable = True
    elif now - state.last_position_monotonic > config.max_position_age:
        reasons.append(FaultKind.POSITION_MESSAGE_MISSING.value)
        attributable = True

    position_reference = state.expected_ecef or state.reference_ecef
    if position_reference is not None and state.reported_ecef is not None:
        if (
            dist(position_reference, state.reported_ecef)
            > config.maximum_position_error
        ):
            reasons.append(FaultKind.POSITION_MISMATCH.value)
            attributable = True
    if (
        state.position_jumped
        and state.position_jump_distance > config.maximum_position_jump
    ):
        reasons.append(FaultKind.POSITION_JUMP.value)
        attributable = True
    if state.station_id_changed:
        reasons.append(FaultKind.STATION_ID_CHANGE.value)
        attributable = True

    satellite_count = len({(key.constellation, key.satellite_id) for key in state.satellites})
    if state.last_msm_by_constellation and satellite_count < config.minimum_satellites:
        reasons.append(FaultKind.SATELLITE_COLLAPSE.value)
        attributable = True

    state.cycle_slips_at[:] = [
        timestamp
        for timestamp in state.cycle_slips_at
        if now - timestamp <= config.cycle_slip_window
    ]
    if len(state.cycle_slips_at) >= config.mass_cycle_slip_count:
        reasons.append(FaultKind.MASS_CYCLE_SLIP.value)
        attributable = True

    current_cnrs = [
        observation.cnr
        for observation in state.satellites.values()
        if observation.cnr is not None
    ]
    current_cnr = median(current_cnrs) if current_cnrs else None
    if current_cnr is not None and current_cnr < config.minimum_cnr:
        reasons.append(FaultKind.CNR_COLLAPSE.value)
        attributable = True
    elif any(
        len(history) >= 10
        and median(history[:-3]) - median(history[-3:]) >= config.cnr_collapse_delta
        for history in state.cnr_history.values()
    ):
        reasons.append(FaultKind.CNR_COLLAPSE.value)
        attributable = True

    hard_reasons = {
        FaultKind.STREAM_LOSS.value,
        FaultKind.POSITION_JUMP.value,
        FaultKind.POSITION_MISMATCH.value,
        FaultKind.STATION_ID_CHANGE.value,
    }
    failed = bool(hard_reasons.intersection(reasons))
    healthy = not reasons and state.last_packet_monotonic is not None
    return SourceHealth(
        healthy=healthy,
        failed=failed,
        attributable=attributable,
        reasons=tuple(dict.fromkeys(reasons)),
        msm_age=msm_age,
        satellite_count=satellite_count,
        median_cnr=current_cnr,
    )
