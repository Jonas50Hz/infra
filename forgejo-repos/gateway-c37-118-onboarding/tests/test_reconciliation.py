"""Tests for immutable MRID and tombstone reconciliation rules."""

from __future__ import annotations

from datetime import datetime, timezone
import unittest

from gateway_c37_118_onboarding.codec import build_source_message, serialize_source_message
from gateway_c37_118_onboarding.config import (
    C37_118V2SignalSelectorDefinition,
    Catalog,
    SignalDefinition,
    SourceDefinition,
)
from gateway_c37_118_onboarding.reconciliation import ReconciliationError, reconcile_catalog


PUBLISHED_AT = datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc)


class ReconciliationTests(unittest.TestCase):
    """Protect published source identity while allowing safe endpoint updates."""

    def test_upserts_current_catalog_sources(self) -> None:
        source = self._source()
        existing = {
            source.source_id: self._payload(source, "old-revision", "wama-c37-118-onboarding")
        }
        catalog = Catalog("wama-c37-118-onboarding", "new-revision", (source,))

        plan = reconcile_catalog(catalog, existing, PUBLISHED_AT)

        self.assertEqual([record.source_id for record in plan.upserts], [source.source_id])
        self.assertEqual(plan.tombstones, ())

    def test_tombstones_removed_catalog_sources(self) -> None:
        source = self._source()
        existing = {
            source.source_id: self._payload(source, "old-revision", "wama-c37-118-onboarding")
        }

        plan = reconcile_catalog(
            Catalog("wama-c37-118-onboarding", "new-revision", ()),
            existing,
            PUBLISHED_AT,
        )

        self.assertEqual(plan.upserts, ())
        self.assertEqual(plan.tombstones, (source.source_id,))

    def test_rejects_mrid_mutation_for_an_existing_signal(self) -> None:
        previous = self._source()
        changed = self._source(mrid="urn:wama:poc:pmu:bay-01:frequency-new")

        with self.assertRaisesRegex(ReconciliationError, "immutable"):
            reconcile_catalog(
                Catalog("wama-c37-118-onboarding", "new-revision", (changed,)),
                {
                    previous.source_id: self._payload(
                        previous,
                        "old-revision",
                        "wama-c37-118-onboarding",
                    )
                },
                PUBLISHED_AT,
            )

    def test_rejects_mrid_owned_by_another_source(self) -> None:
        other = self._source(source_id="pmu-bay-02")
        proposed = self._source()

        with self.assertRaisesRegex(ReconciliationError, "already owned"):
            reconcile_catalog(
                Catalog("wama-c37-118-onboarding", "new-revision", (proposed,)),
                {
                    other.source_id: self._payload(
                        other,
                        "old-revision",
                        "other-catalog",
                    )
                },
                PUBLISHED_AT,
            )

    @staticmethod
    def _payload(source: SourceDefinition, revision: str, catalog_id: str) -> bytes:
        return serialize_source_message(
            build_source_message(source, catalog_id, revision, PUBLISHED_AT)
        )

    @staticmethod
    def _source(
        source_id: str = "pmu-bay-01",
        mrid: str = "urn:wama:poc:pmu:bay-01:frequency",
    ) -> SourceDefinition:
        return SourceDefinition(
            source_id=source_id,
            site_id="wama-poc-bay-01",
            display_name="WAMA PoC Bay 01",
            ip_address="192.0.2.10",
            port=4712,
            pmu_idcode=1001,
            wire_version=2,
            signals=(
                SignalDefinition(
                    signal_id="frequency",
                    source_channel="FREQ",
                    mrid=mrid,
                    value_kind="double",
                    quantity="frequency",
                    unit="Hz",
                    c37_118_v2_selector=C37_118V2SignalSelectorDefinition(
                        kind="frequency"
                    ),
                ),
            ),
        )