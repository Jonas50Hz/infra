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
    """Keep the initial policy aligned with the reviewed five-PMU catalog."""

    def test_declares_each_approved_source_with_exact_sorted_mrids(self) -> None:
        expected = {
            "urn:wama:poc:pmu:bay-01:frequency": (
                "urn:wama:poc:pmu:bay-01:current-l1",
                "urn:wama:poc:pmu:bay-01:current-l2",
                "urn:wama:poc:pmu:bay-01:current-l3",
                "urn:wama:poc:pmu:bay-01:frequency",
                "urn:wama:poc:pmu:bay-01:rocof",
                "urn:wama:poc:pmu:bay-01:voltage-l1",
                "urn:wama:poc:pmu:bay-01:voltage-l2",
                "urn:wama:poc:pmu:bay-01:voltage-l3",
            ),
            "urn:wama:poc:pmu:bay-02:frequency": (
                "urn:wama:poc:pmu:bay-02:current-l1",
                "urn:wama:poc:pmu:bay-02:current-l2",
                "urn:wama:poc:pmu:bay-02:current-l3",
                "urn:wama:poc:pmu:bay-02:frequency",
                "urn:wama:poc:pmu:bay-02:rocof",
                "urn:wama:poc:pmu:bay-02:voltage-l1",
                "urn:wama:poc:pmu:bay-02:voltage-l2",
                "urn:wama:poc:pmu:bay-02:voltage-l3",
            ),
            "urn:wama:poc:pmu:bay-03:frequency": (
                "urn:wama:poc:pmu:bay-03:current-l1",
                "urn:wama:poc:pmu:bay-03:current-l2",
                "urn:wama:poc:pmu:bay-03:current-l3",
                "urn:wama:poc:pmu:bay-03:frequency",
                "urn:wama:poc:pmu:bay-03:rocof",
                "urn:wama:poc:pmu:bay-03:voltage-l1",
                "urn:wama:poc:pmu:bay-03:voltage-l2",
                "urn:wama:poc:pmu:bay-03:voltage-l3",
            ),
            "urn:wama:poc:pmu:bay-04:frequency": (
                "urn:wama:poc:pmu:bay-04:current-l1",
                "urn:wama:poc:pmu:bay-04:current-l2",
                "urn:wama:poc:pmu:bay-04:current-l3",
                "urn:wama:poc:pmu:bay-04:frequency",
                "urn:wama:poc:pmu:bay-04:rocof",
                "urn:wama:poc:pmu:bay-04:voltage-l1",
                "urn:wama:poc:pmu:bay-04:voltage-l2",
                "urn:wama:poc:pmu:bay-04:voltage-l3",
            ),
            "urn:wama:poc:pmu:bay-05:frequency": (
                "urn:wama:poc:pmu:bay-05:current-l1",
                "urn:wama:poc:pmu:bay-05:current-l2",
                "urn:wama:poc:pmu:bay-05:current-l3",
                "urn:wama:poc:pmu:bay-05:frequency",
                "urn:wama:poc:pmu:bay-05:rocof",
                "urn:wama:poc:pmu:bay-05:voltage-l1",
                "urn:wama:poc:pmu:bay-05:voltage-l2",
                "urn:wama:poc:pmu:bay-05:voltage-l3",
            ),
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