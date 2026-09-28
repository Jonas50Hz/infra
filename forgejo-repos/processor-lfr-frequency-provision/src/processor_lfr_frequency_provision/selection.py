"""Deterministic selection of a preferred frequency from PMU aggregates."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
import math


class PmuQuality(IntEnum):
    """Ordered LFR PMU quality classes."""

    BAD = 0
    GOOD = 1
    VERY_GOOD = 2


class EvenMedianTieBreak(IntEnum):
    """Stable selection when central values are equally distant from 50 Hz."""

    LOWER_FREQUENCY = 0
    HIGHER_FREQUENCY = 1


@dataclass(frozen=True)
class PmuAggregate:
    """One PMU's completed frequency result for a UTC second."""

    pmu_id: str
    mean_frequency_hz: float | None
    quality: PmuQuality


@dataclass(frozen=True)
class PreferredFrequency:
    """The eligible highest-quality subset's selected frequency."""

    frequency_hz: float
    quality: PmuQuality
    selected_pmu_ids: tuple[str, ...]


def select_preferred_frequency(
    aggregates: tuple[PmuAggregate, ...],
    tie_break: EvenMedianTieBreak,
) -> PreferredFrequency | None:
    """Select the LFR preferred frequency from the highest eligible PMU class."""

    eligible = tuple(
        aggregate
        for aggregate in aggregates
        if aggregate.quality != PmuQuality.BAD
        and aggregate.mean_frequency_hz is not None
        and math.isfinite(aggregate.mean_frequency_hz)
    )
    if not eligible:
        return None

    selected_quality = max(aggregate.quality for aggregate in eligible)
    selected = tuple(
        aggregate
        for aggregate in eligible
        if aggregate.quality == selected_quality
    )
    frequencies = sorted(
        aggregate.mean_frequency_hz
        for aggregate in selected
        if aggregate.mean_frequency_hz is not None
    )
    return PreferredFrequency(
        frequency_hz=_median_frequency(frequencies, tie_break),
        quality=selected_quality,
        selected_pmu_ids=tuple(sorted(aggregate.pmu_id for aggregate in selected)),
    )


def _median_frequency(
    frequencies: list[float],
    tie_break: EvenMedianTieBreak,
) -> float:
    """Apply the LFR odd/even median rule without averaging even candidates."""

    count = len(frequencies)
    if count % 2:
        return frequencies[count // 2]

    lower = frequencies[(count // 2) - 1]
    upper = frequencies[count // 2]
    lower_deviation = abs(lower - 50.0)
    upper_deviation = abs(upper - 50.0)
    if lower_deviation > upper_deviation:
        return lower
    if upper_deviation > lower_deviation:
        return upper
    if tie_break == EvenMedianTieBreak.LOWER_FREQUENCY:
        return lower
    if tie_break == EvenMedianTieBreak.HIGHER_FREQUENCY:
        return upper
    raise ValueError("tie_break must be an EvenMedianTieBreak")