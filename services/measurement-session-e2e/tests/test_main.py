"""Focused raw-Protobuf checks used by the live contract-to-download service."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
import unittest

from google.protobuf.timestamp_pb2 import Timestamp

from measurement_session_common.contract import DEFAULT_SESSION_BUCKET, MANIFEST_MEDIA_TYPE
from measurement_session_common.generated.measurement_session_pb2 import MeasurementSession
from measurement_session_e2e.main import (
    ContractToDownloadError,
    validate_kafka_record,
    validate_measurement_csv,
)


class KafkaEvidenceTests(unittest.TestCase):
    """Ensure the E2E service independently validates raw Kafka evidence."""

    def test_accepts_matching_finalized_session_key_and_timestamp(self) -> None:
        message = _message()
        record = SimpleNamespace(
            key=message.session_id.encode("utf-8"),
            value=message.SerializeToString(),
            timestamp=message.finalized_at.seconds * 1_000,
        )

        decoded = validate_kafka_record(record, message.session_id)

        self.assertEqual(decoded.session_id, message.session_id)

    def test_rejects_key_mismatch(self) -> None:
        message = _message()
        record = SimpleNamespace(
            key=b"unexpected",
            value=message.SerializeToString(),
            timestamp=message.finalized_at.seconds * 1_000,
        )

        with self.assertRaisesRegex(ContractToDownloadError, "Kafka key"):
            validate_kafka_record(record, message.session_id)

    def test_accepts_complete_measurement_csv(self) -> None:
        validate_measurement_csv(
            (
                b"timestamp,voltage_l1,voltage_l2,voltage_l3\n"
                b"2026-08-18T09:00:00.000000Z,230.100,229.800,230.400\n"
                b"2026-08-18T09:00:02.500000Z,230.050,229.850,230.300\n"
                b"2026-08-18T09:00:05.000000Z,230.000,229.900,230.200\n"
            ),
            {
                "measurement_count": 3,
                "started_at": "2026-08-18T09:00:00Z",
                "ended_at": "2026-08-18T09:00:05Z",
            },
        )

    def test_rejects_boundary_only_measurement_csv(self) -> None:
        with self.assertRaisesRegex(ContractToDownloadError, "every declared measurement"):
            validate_measurement_csv(
                (
                    b"timestamp,voltage_l1,voltage_l2,voltage_l3\n"
                    b"2026-08-18T09:00:00.000000Z,230.100,229.800,230.400\n"
                    b"2026-08-18T09:00:05.000000Z,230.000,229.900,230.200\n"
                ),
                {
                    "measurement_count": 120,
                    "started_at": "2026-08-18T09:00:00Z",
                    "ended_at": "2026-08-18T09:00:05Z",
                },
            )


def _message() -> MeasurementSession:
    message = MeasurementSession(
        session_id="4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237",
        source_mrid="urn:wama:poc:pmu:bay-01",
        measurement_count=1,
        artifact_count=1,
    )
    for field_name, timestamp in (
        ("started_at", datetime(2026, 8, 18, 9, 0, tzinfo=timezone.utc)),
        ("ended_at", datetime(2026, 8, 18, 9, 1, tzinfo=timezone.utc)),
        ("finalized_at", datetime(2026, 8, 18, 9, 2, tzinfo=timezone.utc)),
    ):
        value = Timestamp()
        value.FromDatetime(timestamp)
        getattr(message, field_name).CopyFrom(value)
    message.manifest.bucket = DEFAULT_SESSION_BUCKET
    message.manifest.object_key = f"sessions/{message.session_id}/manifest.json"
    message.manifest.byte_length = 100
    message.manifest.media_type = MANIFEST_MEDIA_TYPE
    message.manifest.sha256 = b"x" * 32
    return message