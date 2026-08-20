"""Tests for the MeasurementSession request and Blobmeta contracts."""

from __future__ import annotations

from datetime import datetime, timezone
import unittest

from google.protobuf.timestamp_pb2 import Timestamp

from measurement_session_common.contract import (
    DEFAULT_SESSION_BUCKET,
    PARQUET_MEDIA_TYPE,
    ContractValidationError,
    rejected_blob_id,
    request_sha256,
    session_parquet_key,
    successful_blob_id,
    validate_blobmeta,
    validate_measurement_session_request,
)
from measurement_session_common.generated.blobmeta_pb2 import Blobmeta
from measurement_session_common.generated.measurement_session_pb2 import MeasurementSessionRequest


class MeasurementSessionContractTests(unittest.TestCase):
    """Prove the request/result boundary remains bounded and immutable."""

    def test_accepts_bounded_request_and_complete_result(self) -> None:
        request = _request()
        validate_measurement_session_request(request)

        result = _completed_result(request)
        validate_blobmeta(result)

    def test_rejects_unsorted_request_mrids(self) -> None:
        request = _request()
        request.mrids[:] = ["urn:wama:poc:b", "urn:wama:poc:a"]

        with self.assertRaisesRegex(ContractValidationError, "strictly sorted"):
            validate_measurement_session_request(request)

    def test_rejects_request_larger_than_configured_interval(self) -> None:
        request = _request()
        _copy_timestamp(request.ended_at, datetime(2026, 8, 20, 9, 1, tzinfo=timezone.utc))

        with self.assertRaisesRegex(ContractValidationError, "configured maximum"):
            validate_measurement_session_request(request)

    def test_accepts_partial_result_with_missing_mrid(self) -> None:
        request = _request()
        result = _completed_result(request)
        result.status = Blobmeta.PARTIAL
        result.mrid_coverage[1].measurement_count = 0
        result.measurement_count = result.mrid_coverage[0].measurement_count

        validate_blobmeta(result)

    def test_rejects_complete_result_with_missing_mrid(self) -> None:
        request = _request()
        result = _completed_result(request)
        result.mrid_coverage[1].measurement_count = 0
        result.measurement_count = result.mrid_coverage[0].measurement_count

        with self.assertRaisesRegex(ContractValidationError, "COMPLETE"):
            validate_blobmeta(result)

    def test_accepts_rejected_result_without_blob(self) -> None:
        request = _request()
        digest = request_sha256(request)
        result = Blobmeta(
            blob_id=rejected_blob_id(request.session_id, digest),
            session_id=request.session_id,
            request_sha256=digest,
            status=Blobmeta.REJECTED,
            rejection_reason="measurement interval exceeds configured maximum",
        )
        for name in ("requested_at", "started_at", "ended_at", "finalized_at"):
            _copy_timestamp(getattr(result, name), getattr(request, name, request.requested_at).ToDatetime(tzinfo=timezone.utc))

        validate_blobmeta(result)


def _request() -> MeasurementSessionRequest:
    request = MeasurementSessionRequest(
        session_id="4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237",
        mrids=("urn:wama:poc:a", "urn:wama:poc:b"),
    )
    _copy_timestamp(request.requested_at, datetime(2026, 8, 19, 9, 2, tzinfo=timezone.utc))
    _copy_timestamp(request.started_at, datetime(2026, 8, 19, 9, 0, tzinfo=timezone.utc))
    _copy_timestamp(request.ended_at, datetime(2026, 8, 19, 9, 1, tzinfo=timezone.utc))
    metadata = request.metadata.add()
    metadata.key = "asset"
    metadata.value = "bay-01"
    return request


def _completed_result(request: MeasurementSessionRequest) -> Blobmeta:
    result = Blobmeta(
        blob_id=successful_blob_id(request.session_id),
        session_id=request.session_id,
        request_sha256=request_sha256(request),
        mrids=request.mrids,
        measurement_count=3,
        status=Blobmeta.COMPLETE,
    )
    for name in ("requested_at", "started_at", "ended_at"):
        getattr(result, name).CopyFrom(getattr(request, name))
    _copy_timestamp(result.finalized_at, datetime(2026, 8, 19, 9, 3, tzinfo=timezone.utc))
    result.metadata.extend(request.metadata)
    first = result.mrid_coverage.add()
    first.mrid = request.mrids[0]
    first.measurement_count = 2
    second = result.mrid_coverage.add()
    second.mrid = request.mrids[1]
    second.measurement_count = 1
    result.object.bucket = DEFAULT_SESSION_BUCKET
    result.object.object_key = session_parquet_key(request.session_id)
    result.object.media_type = PARQUET_MEDIA_TYPE
    result.object.byte_length = 64
    result.object.sha256 = b"x" * 32
    return result


def _copy_timestamp(destination: Timestamp, value: datetime) -> None:
    destination.FromDatetime(value)