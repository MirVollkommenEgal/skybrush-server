"""Comparison of common satellite signals from two stationary bases."""

from __future__ import annotations

from collections import defaultdict
from statistics import median

from .config import HealthConfig
from .model import BaseState, PairHealth, PairState, SatKey


class PairComparator:
    def __init__(self, config: HealthConfig) -> None:
        self._config = config
        self._previous_dd: dict[SatKey, float] = {}
        self._pivots: dict[tuple[str, int], SatKey] = {}
        self._disagree_until: float | None = None

    def compare(self, primary: BaseState, backup: BaseState, now: float) -> PairHealth:
        fresh_constellations = {
            constellation
            for constellation in primary.last_msm_by_constellation.keys()
            & backup.last_msm_by_constellation.keys()
            if now - primary.last_msm_by_constellation[constellation]
            <= self._config.max_msm_age
            and now - backup.last_msm_by_constellation[constellation]
            <= self._config.max_msm_age
        }
        common = {
            key
            for key in primary.satellites.keys() & backup.satellites.keys()
            if key.constellation in fresh_constellations
        }
        common_satellites = len({(key.constellation, key.satellite_id) for key in common})
        denominator = min(
            len({(key.constellation, key.satellite_id) for key in primary.satellites}),
            len({(key.constellation, key.satellite_id) for key in backup.satellites}),
        )
        overlap = common_satellites / denominator if denominator else 0.0
        if (
            common_satellites < self._config.minimum_common_satellites
            or len(common) < self._config.minimum_common_signals
        ):
            return PairHealth(
                PairState.NOT_COMPARABLE,
                ("INSUFFICIENT_COMMON_SATELLITES",),
                common_satellites,
                len(common),
                overlap,
                None,
            )

        if any(
            abs(primary.satellites[key].epoch - backup.satellites[key].epoch)
            > self._config.epoch_tolerance
            for key in common
        ):
            return PairHealth(
                PairState.NOT_COMPARABLE,
                ("EPOCH_MISMATCH",),
                common_satellites,
                len(common),
                overlap,
                None,
            )

        ratio = self._phase_outlier_ratio(common, primary, backup)
        if ratio is not None and ratio > self._config.maximum_phase_outlier_ratio:
            self._disagree_until = now + self._config.pair_disagreement_hold
            return PairHealth(
                PairState.DISAGREE,
                ("PHASE_INSTABILITY",),
                common_satellites,
                len(common),
                overlap,
                ratio,
            )
        if self._disagree_until is not None and now < self._disagree_until:
            return PairHealth(
                PairState.DISAGREE,
                ("RECENT_PHASE_INSTABILITY",),
                common_satellites,
                len(common),
                overlap,
                ratio,
            )
        return PairHealth(
            PairState.CONSISTENT,
            (),
            common_satellites,
            len(common),
            overlap,
            ratio,
        )

    def _phase_outlier_ratio(self, common, primary: BaseState, backup: BaseState):
        if not self._config.enable_phase_comparison:
            return None
        groups: dict[tuple[str, int], list[SatKey]] = defaultdict(list)
        for key in common:
            if (
                primary.satellites[key].phase_range is not None
                and backup.satellites[key].phase_range is not None
            ):
                groups[(key.constellation, key.signal_id)].append(key)

        temporal: list[float] = []
        next_dd: dict[SatKey, float] = {}
        for group, keys in groups.items():
            if len(keys) < 2:
                continue
            pivot = max(
                keys,
                key=lambda key: (
                    min(
                        primary.satellites[key].lock_time,
                        backup.satellites[key].lock_time,
                    ),
                    min(
                        primary.satellites[key].cnr or 0,
                        backup.satellites[key].cnr or 0,
                    ),
                ),
            )
            if self._pivots.get(group) not in keys:
                self._pivots[group] = pivot
            else:
                pivot = self._pivots[group]
            p_delta = (
                backup.satellites[pivot].phase_range
                - primary.satellites[pivot].phase_range
            )
            for key in keys:
                if key == pivot:
                    continue
                dd = (
                    backup.satellites[key].phase_range
                    - primary.satellites[key].phase_range
                    - p_delta
                )
                next_dd[key] = dd
                if key in self._previous_dd:
                    temporal.append(dd - self._previous_dd[key])
        self._previous_dd = next_dd
        if len(temporal) < 3:
            return None
        center = median(temporal)
        mad = median(abs(value - center) for value in temporal)
        threshold = max(
            self._config.phase_absolute_floor,
            self._config.phase_mad_multiplier * mad,
        )
        return sum(abs(value - center) > threshold for value in temporal) / len(temporal)
