"""Tests for bounded immutable finalized-session contract enforcement."""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import unittest

from google.protobuf.timestamp_pb2 import Timestamp

from measurement_session_common.contract import (
    DEFAULT_SESSION_BUCKET,
    MANIFEST_MEDIA_TYPE,
    ContractValidationError,
    validate_measurement_session,
)
from measurement_session_common.generated.measurement_session_pb2 import MeasurementSession
from measurement_session_common.manifest import ArtifactDescriptor, canonical_manifest_bytes, parse_manifest


class MeasurementSessionContractTests(unittest.TestCase):
    """Ensure raw-Protobuf records cannot represent mutable or unsafe sessions."""

    def test_accepts_a_bounded_finalized_session(self) -> None:
        session = self._session()

        validate_measurement_session(session)

    def test_rejects_unsorted_or_duplicate_metadata(self) -> None:
        session = self._session()
        duplicate = session.metadata.add()
        duplicate.key = "asset"
        duplicate.value = "bay-02"

        with self.assertRaisesRegex(ContractValidationError, "strictly sorted"):
            validate_measurement_session(session)

    def test_manifest_round_trip_is_canonical_and_session_scoped(self) -> None:
        session_id = "4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237"
        artifact = ArtifactDescriptor(
            artifact_id="waveform",
            object_key=f"sessions/{session_id}/artifacts/waveform.csv",
            content_type="text/csv",
            size_bytes=3,
            sha256_hex=sha256(b"a,b").hexdigest(),
        )

        payload = canonical_manifest_bytes(session_id, (artifact,))
        manifest = parse_manifest(payload, session_id)

        self.assertEqual(manifest.artifacts, (artifact,))

    def _session(self) -> MeasurementSession:
        session_id = "4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237"
        session = MeasurementSession(
            session_id=session_id,
            source_mrid="urn:wama:poc:pmu:bay-01",
            measurement_count=12,
            artifact_count=1,
        )
        for name, timestamp in (
            ("started_at", datetime(2026, 8, 18, 9, 0, tzinfo=timezone.utc)),
            ("ended_at", datetime(2026, 8, 18, 9, 1, tzinfo=timezone.utc)),
            ("finalized_at", datetime(2026, 8, 18, 9, 2, tzinfo=timezone.utc)),
        ):
            value = Timestamp()
            value.FromDatetime(timestamp)
            getattr(session, name).CopyFrom(value)
        metadata = session.metadata.add()
        metadata.key = "asset"
        metadata.value = "bay-01"
        session.manifest.bucket = DEFAULT_SESSION_BUCKET
        session.manifest.object_key = f"sessions/{session_id}/manifest.json"
        session.manifest.byte_length = 100
        session.manifest.media_type = MANIFEST_MEDIA_TYPE
        session.manifest.sha256 = b"x" * 32
        return session