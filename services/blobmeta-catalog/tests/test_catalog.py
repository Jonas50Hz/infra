"""Tests for raw-Protobuf Blobmeta translation into immutable catalog records."""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
import unittest

from google.protobuf.timestamp_pb2 import Timestamp

from blobmeta_catalog.catalog import CatalogBlob
from measurement_session_common.contract import (
    DEFAULT_SESSION_BUCKET,
    PARQUET_MEDIA_TYPE,
    request_sha256,
    session_parquet_key,
    successful_blob_id,
)
from measurement_session_common.generated.blobmeta_pb2 import Blobmeta
from measurement_session_common.generated.measurement_session_pb2 import MeasurementSessionRequest


class CatalogBlobTests(unittest.TestCase):
    """Ensure catalog projections retain the original contract digest and coverage."""

    def test_builds_projection_from_valid_blobmeta(self) -> None:
        request = _request()
        message = _blobmeta(request)
        payload = message.SerializeToString(deterministic=True)

        blob = CatalogBlob.from_protobuf(message, payload)

        self.assertEqual(blob.contract_sha256, sha256(payload).digest())
        self.assertEqual(blob.mrids, (("urn:wama:poc:a", 2),))
        self.assertEqual(blob.object.object_key if blob.object else None, session_parquet_key(request.session_id))


def _request() -> MeasurementSessionRequest:
    request = MeasurementSessionRequest(
        session_id="4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237",
        mrids=("urn:wama:poc:a",),
    )
    for field_name, value in (
        ("requested_at", datetime(2026, 8, 19, 9, 2, tzinfo=timezone.utc)),
        ("started_at", datetime(2026, 8, 19, 9, 0, tzinfo=timezone.utc)),
        ("ended_at", datetime(2026, 8, 19, 9, 1, tzinfo=timezone.utc)),
    ):
        timestamp = Timestamp()
        timestamp.FromDatetime(value)
        getattr(request, field_name).CopyFrom(timestamp)
    return request


def _blobmeta(request: MeasurementSessionRequest) -> Blobmeta:
    message = Blobmeta(
        blob_id=successful_blob_id(request.session_id),
        session_id=request.session_id,
        request_sha256=request_sha256(request),
        mrids=request.mrids,
        measurement_count=2,
        status=Blobmeta.COMPLETE,
    )
    for field_name in ("requested_at", "started_at", "ended_at"):
        getattr(message, field_name).CopyFrom(getattr(request, field_name))
    finalized = Timestamp()
    finalized.FromDatetime(datetime(2026, 8, 19, 9, 3, tzinfo=timezone.utc))
    message.finalized_at.CopyFrom(finalized)
    coverage = message.mrid_coverage.add()
    coverage.mrid = request.mrids[0]
    coverage.measurement_count = 2
    message.object.bucket = DEFAULT_SESSION_BUCKET
    message.object.object_key = session_parquet_key(request.session_id)
    message.object.media_type = PARQUET_MEDIA_TYPE
    message.object.byte_length = 64
    message.object.sha256 = b"x" * 32
    return message