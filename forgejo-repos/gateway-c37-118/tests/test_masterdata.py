"""Masterdata wire and verifier tests."""

from __future__ import annotations

from datetime import datetime, timezone
import unittest

from google.protobuf.timestamp_pb2 import Timestamp

from gateway_c37_118.catalog import Catalog, Signal, Source
from gateway_c37_118.masterdata import (
    MasterdataError,
    _validate_existing_record,
    encode_source,
    reconcile,
    verify_catalog_projection,
)
from gateway_c37_118.verify_live_measurements import VerificationError, verify_record
from gateway_c37_118.generated import masterdata_pb2, rtd_schema_pb2

DEMO_SOURCE = Source(
    source_id="pmu-demo",
    site_id="demo",
    display_name="Demo PMU",
    ip_address="127.0.0.1",
    port=4712,
    pmu_idcode=1001,
    signals=(
        Signal(
            signal_id="frequency",
            source_channel="FREQ",
            mrid="urn:wama:demo:pmu:frequency",
            quantity="frequency",
            unit="Hz",
            selector="frequency",
        ),
    ),
)
REMOVED_SOURCE = Source(
    source_id="pmu-removed",
    site_id="removed",
    display_name="Removed PMU",
    ip_address="127.0.0.1",
    port=4713,
    pmu_idcode=1002,
    signals=(
        Signal(
            signal_id="frequency",
            source_channel="FREQ",
            mrid="urn:wama:demo:pmu:removed:frequency",
            quantity="frequency",
            unit="Hz",
            selector="frequency",
        ),
    ),
)
CATALOG = Catalog(catalog_id="c37-118-poc-v1", sources=(DEMO_SOURCE,))


class _Delivery:
    def get(self, timeout: float | None = None) -> None:
        return None


class _Producer:
    def __init__(self) -> None:
        self.messages: list[tuple[str, bytes, bytes | None]] = []

    def send(self, topic: str, *, key: bytes, value: bytes | None) -> _Delivery:
        self.messages.append((topic, key, value))
        return _Delivery()


class MasterdataTests(unittest.TestCase):
    """Ensure raw Protobuf output has deterministic content and valid proofs."""

    def test_encodes_reviewed_source_deterministically(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        encoded = encode_source(DEMO_SOURCE, CATALOG.catalog_id, "abc", now)
        self.assertEqual(
            encoded,
            encode_source(DEMO_SOURCE, CATALOG.catalog_id, "abc", now),
        )
        record = masterdata_pb2.SourceMasterdata()
        record.ParseFromString(encoded)
        self.assertEqual(record.source_id, DEMO_SOURCE.source_id)
        self.assertEqual(len(record.signals), len(DEMO_SOURCE.signals))

    def test_verifier_requires_key_quality_and_timestamp_order(self) -> None:
        record = rtd_schema_pb2.MCCSMeasurementValue(mrid="urn:test", double_value=50.0)
        for timestamp, seconds in (
            (record.timestamp_field, 1),
            (record.timestamp_gateway, 2),
            (record.timestamp_mccs, 3),
        ):
            timestamp.CopyFrom(Timestamp(seconds=seconds))
        record.quality.valid = False
        expected = frozenset({"urn:test"})
        self.assertEqual(verify_record(expected, b"urn:test", record.SerializeToString()), "urn:test")
        with self.assertRaises(VerificationError):
            verify_record(expected, b"other", record.SerializeToString())
        record.ClearField("timestamp_mccs")
        with self.assertRaisesRegex(VerificationError, "incomplete timestamps"):
            verify_record(expected, b"urn:test", record.SerializeToString())

    def test_retained_masterdata_requires_published_at(self) -> None:
        record = masterdata_pb2.SourceMasterdata()
        record.ParseFromString(
            encode_source(
                DEMO_SOURCE,
                CATALOG.catalog_id,
                "abc",
                datetime(2026, 1, 1, tzinfo=timezone.utc),
            )
        )
        record.ClearField("published_at")
        with self.assertRaisesRegex(MasterdataError, "Malformed"):
            _validate_existing_record(record, DEMO_SOURCE.source_id)

    def test_projection_requires_catalog_metadata_and_signals(self) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        projection = {
            source.source_id: encode_source(source, CATALOG.catalog_id, "abc", now)
            for source in CATALOG.sources
        }
        self.assertEqual(
            verify_catalog_projection(projection, CATALOG, "abc"),
            frozenset(source.source_id for source in CATALOG.sources),
        )
        with self.assertRaisesRegex(MasterdataError, "catalog metadata"):
            verify_catalog_projection(projection, CATALOG, "different")

    def test_removed_source_is_tombstoned_while_remaining_source_is_published(
        self,
    ) -> None:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        producer = _Producer()
        existing = {
            DEMO_SOURCE.source_id: encode_source(
                DEMO_SOURCE,
                CATALOG.catalog_id,
                "old",
                now,
            ),
            REMOVED_SOURCE.source_id: encode_source(
                REMOVED_SOURCE,
                CATALOG.catalog_id,
                "old",
                now,
            ),
        }

        changed = reconcile(producer, "Masterdata", CATALOG, "new", existing, now)

        self.assertEqual(changed, [DEMO_SOURCE.source_id, REMOVED_SOURCE.source_id])
        self.assertEqual(
            producer.messages,
            [
                (
                    "Masterdata",
                    DEMO_SOURCE.source_id.encode("utf-8"),
                    encode_source(DEMO_SOURCE, CATALOG.catalog_id, "new", now),
                ),
                ("Masterdata", REMOVED_SOURCE.source_id.encode("utf-8"), None),
            ],
        )


if __name__ == "__main__":
    unittest.main()
