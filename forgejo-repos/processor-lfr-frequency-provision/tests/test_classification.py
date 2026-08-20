"""Tests for configurable LFR PMU quality boundaries."""

from __future__ import annotations

import unittest

from processor_lfr_frequency_provision.classification import (
    CountThresholds,
    VoltageThresholds,
    worst_quality,
)
from processor_lfr_frequency_provision.selection import PmuQuality


class ClassificationTests(unittest.TestCase):
    """Require all source-document boundary gaps to be configured explicitly."""

    def test_count_policy_makes_the_10_and_25_boundaries_explicit(self) -> None:
        thresholds = CountThresholds(
            bad_maximum_good_samples=9,
            very_good_minimum_good_samples=26,
        )

        self.assertEqual(thresholds.classify(9), PmuQuality.BAD)
        self.assertEqual(thresholds.classify(10), PmuQuality.GOOD)
        self.assertEqual(thresholds.classify(25), PmuQuality.GOOD)
        self.assertEqual(thresholds.classify(26), PmuQuality.VERY_GOOD)

    def test_voltage_policy_matches_the_documented_strict_and_inclusive_bounds(self) -> None:
        thresholds = VoltageThresholds(
            very_good_maximum_deviation=1.0,
            good_maximum_deviation=2.0,
        )

        self.assertEqual(thresholds.classify(0.99), PmuQuality.VERY_GOOD)
        self.assertEqual(thresholds.classify(1.0), PmuQuality.GOOD)
        self.assertEqual(thresholds.classify(-2.0), PmuQuality.GOOD)
        self.assertEqual(thresholds.classify(2.01), PmuQuality.BAD)
        self.assertEqual(thresholds.classify(None), PmuQuality.BAD)

    def test_rejects_incomplete_or_overlapping_policy_boundaries(self) -> None:
        with self.assertRaisesRegex(ValueError, "exceed"):
            CountThresholds(10, 10)
        with self.assertRaisesRegex(ValueError, "not be lower"):
            VoltageThresholds(2.0, 1.0)

    def test_uses_the_worst_stage_class(self) -> None:
        self.assertEqual(
            worst_quality(PmuQuality.VERY_GOOD, PmuQuality.GOOD),
            PmuQuality.GOOD,
        )
        self.assertEqual(
            worst_quality(PmuQuality.VERY_GOOD, PmuQuality.BAD),
            PmuQuality.BAD,
        )


if __name__ == "__main__":
    unittest.main()