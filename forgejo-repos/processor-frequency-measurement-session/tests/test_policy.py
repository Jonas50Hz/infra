"""Tests for the EE-editable reviewed frequency capture policy."""

from __future__ import annotations

import unittest

from processor_frequency_measurement_session.policy import (
    CAPTURE_POLICIES,
    CAPTURE_REASON,
    FREQUENCY_THRESHOLD_HZ,
    REQUEST_ORIGIN,
    SERVICE_NAME,
    policy_for,
)


class PolicyTests(unittest.TestCase):
    """Keep the initial policy aligned with the reviewed three-PMU catalog."""

    def test_declares_each_approved_source_with_exact_sorted_mrids(self) -> None:
        expected = {
            f"urn:wama:poc:pmu:bay-{bay}:frequency": (
                f"urn:wama:poc:pmu:bay-{bay}:current-phase-a",
                f"urn:wama:poc:pmu:bay-{bay}:current-phase-b",
                f"urn:wama:poc:pmu:bay-{bay}:current-phase-c",
                f"urn:wama:poc:pmu:bay-{bay}:frequency",
                f"urn:wama:poc:pmu:bay-{bay}:rocof",
                f"urn:wama:poc:pmu:bay-{bay}:voltage-phase-a",
                f"urn:wama:poc:pmu:bay-{bay}:voltage-phase-b",
                f"urn:wama:poc:pmu:bay-{bay}:voltage-phase-c",
            )
            for bay in ("01", "02", "03")
        }

        self.assertEqual(
            {policy.frequency_mrid: policy.capture_mrids for policy in CAPTURE_POLICIES},
            expected,
        )
        for frequency_mrid, capture_mrids in expected.items():
            self.assertEqual(capture_mrids, tuple(sorted(capture_mrids)))
            self.assertEqual(policy_for(frequency_mrid).capture_mrids, capture_mrids)

    def test_rejects_unreviewed_frequency_mrid(self) -> None:
        self.assertIsNone(policy_for("urn:wama:poc:pmu:bay-06:frequency"))

    def test_declares_fixed_trigger_metadata(self) -> None:
        self.assertEqual(SERVICE_NAME, "processor-frequency-measurement-session")
        self.assertEqual(REQUEST_ORIGIN, SERVICE_NAME)
        self.assertEqual(CAPTURE_REASON, "frequency_gt_50_2_hz")
        self.assertEqual(FREQUENCY_THRESHOLD_HZ, 50.2)


if __name__ == "__main__":
    unittest.main()