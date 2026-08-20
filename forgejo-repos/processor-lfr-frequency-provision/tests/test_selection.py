"""Tests for the LFR preferred-frequency selection rule."""

from __future__ import annotations

import unittest

from processor_lfr_frequency_provision.selection import (
    EvenMedianTieBreak,
    PmuAggregate,
    PmuQuality,
    select_preferred_frequency,
)


class PreferredFrequencySelectionTests(unittest.TestCase):
    """Keep the configured LFR selection behavior deterministic."""

    def test_uses_only_the_highest_eligible_quality_class(self) -> None:
        result = select_preferred_frequency(
            (
                PmuAggregate("pmu-good", 49.8, PmuQuality.GOOD),
                PmuAggregate("pmu-very-good-a", 49.99, PmuQuality.VERY_GOOD),
                PmuAggregate("pmu-very-good-b", 50.01, PmuQuality.VERY_GOOD),
                PmuAggregate("pmu-bad", 50.3, PmuQuality.BAD),
            ),
            EvenMedianTieBreak.LOWER_FREQUENCY,
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.quality, PmuQuality.VERY_GOOD)
        self.assertEqual(result.frequency_hz, 49.99)
        self.assertEqual(result.selected_pmu_ids, ("pmu-very-good-a", "pmu-very-good-b"))

    def test_uses_the_even_median_farthest_from_50_hz(self) -> None:
        result = select_preferred_frequency(
            (
                PmuAggregate("pmu-a", 49.98, PmuQuality.GOOD),
                PmuAggregate("pmu-b", 50.06, PmuQuality.GOOD),
            ),
            EvenMedianTieBreak.LOWER_FREQUENCY,
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.frequency_hz, 50.06)

    def test_uses_the_configured_equal_distance_tie_break(self) -> None:
        aggregates = (
            PmuAggregate("pmu-a", 49.9, PmuQuality.GOOD),
            PmuAggregate("pmu-b", 50.1, PmuQuality.GOOD),
        )

        lower = select_preferred_frequency(
            aggregates,
            EvenMedianTieBreak.LOWER_FREQUENCY,
        )
        higher = select_preferred_frequency(
            aggregates,
            EvenMedianTieBreak.HIGHER_FREQUENCY,
        )

        self.assertIsNotNone(lower)
        self.assertIsNotNone(higher)
        assert lower is not None
        assert higher is not None
        self.assertEqual(lower.frequency_hz, 49.9)
        self.assertEqual(higher.frequency_hz, 50.1)

    def test_returns_no_result_without_an_eligible_pmu(self) -> None:
        result = select_preferred_frequency(
            (
                PmuAggregate("pmu-bad", 50.0, PmuQuality.BAD),
                PmuAggregate("pmu-missing", None, PmuQuality.VERY_GOOD),
            ),
            EvenMedianTieBreak.LOWER_FREQUENCY,
        )

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()