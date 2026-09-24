"""Tests for scoped dynamic Masterdata membership."""

from __future__ import annotations

import unittest

from processor_alarm_threshold.generated import masterdata_pb2
from processor_alarm_threshold.masterdata import MasterdataError, MasterdataRegistry

from support import CATALOG_ID, FREQUENCY_MRID, reviewed_rules, source


class MasterdataRegistryTests(unittest.TestCase):
    """Only eligible reviewed catalog signals become alarm members."""

    def test_scoped_catalog_tracks_frequency_membership_and_tombstones(self) -> None:
        registry = MasterdataRegistry(CATALOG_ID, reviewed_rules())

        outside = registry.apply(
            b"another-source",
            source("another-source", catalog_id="another-catalog"),
        )
        added = registry.apply(b"pmu-bay-01", source("pmu-bay-01"))
        removed = registry.apply(b"pmu-bay-01", None)

        self.assertEqual(outside.added, frozenset())
        self.assertEqual(added.added, frozenset({FREQUENCY_MRID}))
        self.assertEqual(removed.removed, frozenset({FREQUENCY_MRID}))
        self.assertEqual(registry.mrids, frozenset())

    def test_rejects_incompatible_signal_semantics_in_the_scoped_catalog(self) -> None:
        registry = MasterdataRegistry(CATALOG_ID, reviewed_rules())

        with self.assertRaisesRegex(MasterdataError, "double/frequency/Hz"):
            registry.apply(
                b"pmu-bay-01",
                source(
                    "pmu-bay-01",
                    value_kind=masterdata_pb2.MCCS_VALUE_KIND_INT,
                ),
            )

        self.assertEqual(registry.mrids, frozenset())

    def test_rejects_duplicate_exact_mrids_across_scoped_sources(self) -> None:
        registry = MasterdataRegistry(CATALOG_ID, reviewed_rules())
        registry.apply(b"pmu-bay-01", source("pmu-bay-01"))

        with self.assertRaisesRegex(MasterdataError, "multiple sources"):
            registry.apply(b"pmu-bay-02", source("pmu-bay-02"))

        self.assertEqual(registry.mrids, frozenset({FREQUENCY_MRID}))


if __name__ == "__main__":
    unittest.main()