"""Tests for raw request and Blobmeta e2e record assertions."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
import unittest

from measurement_session_common.contract import (
    DEFAULT_SESSION_BUCKET,
    PARQUET_MEDIA_TYPE,
    request_sha256,
    session_parquet_key,
    successful_blob_id,
)
from measurement_session_common.generated.blobmeta_pb2 import Blobmeta
from measurement_session_e2e.request_flow import (
    RequestFlowError,
    _normalized_parquet_coverage,
    build_request,
    validate_blobmeta_record,
)


class RequestFlowTests(unittest.TestCase):
    """Verify test producers use the same contract checks as real producers."""

    def test_builds_sorted_bounded_request(self) -> None:
        request = build_request(
            "4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237",
            datetime(2026, 8, 19, 9, 2, tzinfo=timezone.utc),
            datetime(2026, 8, 19, 9, 0, tzinfo=timezone.utc),
            datetime(2026, 8, 19, 9, 1, tzinfo=timezone.utc),
            ("urn:wama:poc:a",),
        )

        self.assertEqual(request.metadata[0].key, "capture_reason")

    def test_validates_blobmeta_kafka_timestamp(self) -> None:
        request = build_request(
            "4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237",
            datetime(2026, 8, 19, 9, 2, tzinfo=timezone.utc),
            datetime(2026, 8, 19, 9, 0, tzinfo=timezone.utc),
            datetime(2026, 8, 19, 9, 1, tzinfo=timezone.utc),
            ("urn:wama:poc:a",),
        )
        result = Blobmeta(
            blob_id=successful_blob_id(request.session_id),
            session_id=request.session_id,
            request_sha256=request_sha256(request),
            mrids=request.mrids,
            measurement_count=1,
            status=Blobmeta.COMPLETE,
        )
        result.requested_at.CopyFrom(request.requested_at)
        result.started_at.CopyFrom(request.started_at)
        result.ended_at.CopyFrom(request.ended_at)
        result.finalized_at.CopyFrom(request.requested_at)
        coverage = result.mrid_coverage.add()
        coverage.mrid = request.mrids[0]
        coverage.measurement_count = 1
        result.object.bucket = DEFAULT_SESSION_BUCKET
        result.object.object_key = session_parquet_key(request.session_id)
        result.object.media_type = PARQUET_MEDIA_TYPE
        result.object.byte_length = 1
        result.object.sha256 = b"x" * 32
        timestamp = result.finalized_at.seconds * 1_000 + result.finalized_at.nanos // 1_000_000

        decoded = validate_blobmeta_record(
            SimpleNamespace(
                key=result.blob_id.encode("utf-8"),
                value=result.SerializeToString(deterministic=True),
                timestamp=timestamp,
            )
        )

        self.assertEqual(decoded.blob_id, result.blob_id)

    def test_normalizes_missing_partial_mrid_to_zero_rows(self) -> None:
        coverage = _normalized_parquet_coverage(
            ["urn:wama:poc:a", "urn:wama:poc:a"],
            {"urn:wama:poc:a": 2, "urn:wama:poc:missing": 0},
        )

        self.assertEqual(coverage, {"urn:wama:poc:a": 2, "urn:wama:poc:missing": 0})

    def test_rejects_parquet_mrid_missing_from_blobmeta(self) -> None:
        with self.assertRaisesRegex(RequestFlowError, "not present in Blobmeta"):
            _normalized_parquet_coverage(
                ["urn:wama:poc:unexpected"],
                {"urn:wama:poc:a": 0},
            )