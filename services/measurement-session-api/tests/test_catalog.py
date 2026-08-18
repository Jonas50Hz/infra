"""Tests for raw-Protobuf catalog projection fields."""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import unittest

from google.protobuf.timestamp_pb2 import Timestamp

from measurement_session_common.contract import DEFAULT_SESSION_BUCKET, MANIFEST_MEDIA_TYPE
from measurement_session_common.generated.measurement_session_pb2 import MeasurementSession
from measurement_session_api.catalog import CatalogSession


class CatalogSessionTests(unittest.TestCase):
    """Ensure the catalog uses the original Kafka bytes as immutable evidence."""

    def test_builds_projection_from_valid_raw_protobuf(self) -> None:
        message = MeasurementSession(
            session_id="4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237",
            source_mrid="urn:wama:poc:pmu:bay-01",
            measurement_count=12,
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
        metadata = message.metadata.add()
        metadata.key = "asset"
        metadata.value = "bay-01"
        message.manifest.bucket = DEFAULT_SESSION_BUCKET
        message.manifest.object_key = f"sessions/{message.session_id}/manifest.json"
        message.manifest.byte_length = 100
        message.manifest.media_type = MANIFEST_MEDIA_TYPE
        message.manifest.sha256 = b"x" * 32
        payload = message.SerializeToString()

        session = CatalogSession.from_protobuf(message, payload)

        self.assertEqual(session.contract_sha256, sha256(payload).digest())
        self.assertEqual(session.metadata, (("asset", "bay-01"),))