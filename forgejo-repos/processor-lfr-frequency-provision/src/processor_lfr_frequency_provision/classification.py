"""Configured LFR PMU quality classification policies."""

from __future__ import annotations

from dataclasses import dataclass
import math

from processor_lfr_frequency_provision.selection import PmuQuality


@dataclass(frozen=True)
class CountThresholds:
    """Complete, non-overlapping good-sample count classification policy."""

    bad_maximum_good_samples: int
    very_good_minimum_good_samples: int

    def __post_init__(self) -> None:
        if self.bad_maximum_good_samples < 0:
            raise ValueError("bad_maximum_good_samples must be non-negative")
        if self.very_good_minimum_good_samples <= self.bad_maximum_good_samples:
            raise ValueError(
                "very_good_minimum_good_samples must exceed bad_maximum_good_samples"
            )

    def classify(self, good_sample_count: int) -> PmuQuality:
        """Classify a PMU count using the approved inclusive boundary policy."""

        if good_sample_count < 0:
            raise ValueError("good_sample_count must be non-negative")
        if good_sample_count <= self.bad_maximum_good_samples:
            return PmuQuality.BAD
        if good_sample_count >= self.very_good_minimum_good_samples:
            return PmuQuality.VERY_GOOD
        return PmuQuality.GOOD


@dataclass(frozen=True)
class VoltageThresholds:
    """Voltage-deviation policy for one configured PMU voltage level."""

    very_good_maximum_deviation: float
    good_maximum_deviation: float

    def __post_init__(self) -> None:
        if not math.isfinite(self.very_good_maximum_deviation):
            raise ValueError("very_good_maximum_deviation must be finite")
        if not math.isfinite(self.good_maximum_deviation):
            raise ValueError("good_maximum_deviation must be finite")
        if self.very_good_maximum_deviation < 0:
            raise ValueError("very_good_maximum_deviation must be non-negative")
        if self.good_maximum_deviation < self.very_good_maximum_deviation:
            raise ValueError(
                "good_maximum_deviation must not be lower than very_good_maximum_deviation"
            )

    def classify(self, deviation: float | None) -> PmuQuality:
        """Classify an absolute deviation according to the LFR voltage rule."""

        if deviation is None or not math.isfinite(deviation):
            return PmuQuality.BAD
        absolute_deviation = abs(deviation)
        if absolute_deviation < self.very_good_maximum_deviation:
            return PmuQuality.VERY_GOOD
        if absolute_deviation <= self.good_maximum_deviation:
            return PmuQuality.GOOD
        return PmuQuality.BAD


def worst_quality(*qualities: PmuQuality) -> PmuQuality:
    """Return the least usable class from the supplied classification stages."""

    if not qualities:
        raise ValueError("at least one quality class is required")
    return min(qualities)