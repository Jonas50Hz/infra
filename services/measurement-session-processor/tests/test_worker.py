"""Tests for idempotent request-to-Blobmeta worker behavior."""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO
from types import SimpleNamespace
import unittest

from google.protobuf.timestamp_pb2 import Timestamp

from measurement_session_common.contract import SESSION_PARQUET_SCHEMA_VERSION
from measurement_session_common.generated.blobmeta_pb2 import Blobmeta
from measurement_session_common.generated.measurement_session_pb2 import MeasurementSessionRequest
from measurement_session_processor.config import Settings
from measurement_session_processor.druid import MeasurementRow
from measurement_session_processor.worker import SessionWorker


class SessionWorkerTests(unittest.TestCase):
    """Prove replay, partial coverage, and rejection remain auditable."""

    def test_materializes_once_and_republishes_identical_replay(self) -> None:
        settings = Settings.from_environment({"MEASUREMENT_SESSION_MAX_ROWS": "10"})
        druid = _Druid([_row("urn:wama:poc:a"), _row("urn:wama:poc:b")])
        storage = _Storage()
        producer = _Producer()
        worker = SessionWorker(settings, druid, storage, producer, clock=_clock)
        request = _request(("urn:wama:poc:a", "urn:wama:poc:b"))
        record = _record(request)

        first = worker.process_record(record)
        replay = worker.process_record(record)

        self.assertEqual(first.status, Blobmeta.COMPLETE)
        self.assertEqual(first.object.parquet_schema_version, SESSION_PARQUET_SCHEMA_VERSION)
        self.assertEqual(first.SerializeToString(deterministic=True), replay.SerializeToString(deterministic=True))
        self.assertEqual(druid.calls, 1)
        self.assertEqual(len(producer.records), 2)

    def test_records_partial_when_an_mrid_has_no_rows(self) -> None:
        settings = Settings.from_environment({})
        worker = SessionWorker(settings, _Druid([_row("urn:wama:poc:a")]), _Storage(), _Producer(), _clock)
        request = _request(("urn:wama:poc:a", "urn:wama:poc:b"))

        result = worker.process_record(_record(request))

        self.assertEqual(result.status, Blobmeta.PARTIAL)
        self.assertEqual([item.measurement_count for item in result.mrid_coverage], [1, 0])

    def test_records_rejection_for_bounded_validation_failure(self) -> None:
        settings = Settings.from_environment({"MEASUREMENT_SESSION_MAX_INTERVAL_HOURS": "1"})
        druid = _Druid([])
        worker = SessionWorker(settings, druid, _Storage(), _Producer(), _clock)
        request = _request(("urn:wama:poc:a",), end=datetime(2026, 8, 19, 11, tzinfo=timezone.utc))

        result = worker.process_record(_record(request))

        self.assertEqual(result.status, Blobmeta.REJECTED)
        self.assertEqual(druid.calls, 0)


class _Druid:
    def __init__(self, rows: list[MeasurementRow]) -> None:
        self._rows = rows
        self.calls = 0

    def iter_rows(self, request: MeasurementSessionRequest):
        del request
        self.calls += 1
        yield from self._rows


class _Storage:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], tuple[bytes, str, str]] = {}

    def read_receipt(self, bucket: str, object_key: str) -> bytes | None:
        item = self.objects.get((bucket, object_key))
        return None if item is None else item[0]

    def put_or_verify_file(
        self,
        bucket: str,
        object_key: str,
        path,
        content_type: str,
        digest: bytes,
        size_bytes: int,
    ) -> None:
        payload = path.read_bytes()
        self._put_or_verify(bucket, object_key, payload, content_type, digest, size_bytes)

    def put_or_verify_bytes(
        self,
        bucket: str,
        object_key: str,
        payload: bytes,
        content_type: str,
    ) -> None:
        self._put_or_verify(bucket, object_key, payload, content_type, sha256(payload).digest(), len(payload))

    def verify_object(
        self,
        bucket: str,
        object_key: str,
        content_type: str,
        digest: bytes,
        size_bytes: int,
    ) -> None:
        payload, actual_content_type, _ = self.objects[(bucket, object_key)]
        if actual_content_type != content_type or len(payload) != size_bytes or sha256(payload).digest() != digest:
            raise AssertionError("Object verification failed")

    def _put_or_verify(
        self,
        bucket: str,
        object_key: str,
        payload: bytes,
        content_type: str,
        digest: bytes,
        size_bytes: int,
    ) -> None:
        self.verify_or_store(bucket, object_key, payload, content_type)
        self.verify_object(bucket, object_key, content_type, digest, size_bytes)

    def verify_or_store(self, bucket: str, object_key: str, payload: bytes, content_type: str) -> None:
        existing = self.objects.get((bucket, object_key))
        if existing is not None:
            if existing[0] != payload or existing[1] != content_type:
                raise AssertionError("Immutable object changed")
            return
        self.objects[(bucket, object_key)] = (payload, content_type, sha256(payload).hexdigest())


class _Future:
    def get(self, timeout: int) -> None:
        del timeout


class _Producer:
    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []

    def send(self, topic: str, **kwargs: object) -> _Future:
        self.records.append({"topic": topic, **kwargs})
        return _Future()


def _request(mrids: tuple[str, ...], end: datetime | None = None) -> MeasurementSessionRequest:
    request = MeasurementSessionRequest(
        session_id="4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237",
        mrids=mrids,
    )
    _timestamp(request.requested_at, datetime(2026, 8, 19, 9, 2, tzinfo=timezone.utc))
    _timestamp(request.started_at, datetime(2026, 8, 19, 9, 0, tzinfo=timezone.utc))
    _timestamp(request.ended_at, end or datetime(2026, 8, 19, 9, 1, tzinfo=timezone.utc))
    return request


def _record(request: MeasurementSessionRequest) -> SimpleNamespace:
    return SimpleNamespace(
        key=request.session_id.encode("utf-8"),
        value=request.SerializeToString(deterministic=True),
    )


def _row(mrid: str) -> MeasurementRow:
    return MeasurementRow(
        timestamp_mccs=datetime(2026, 8, 19, 9, 0, tzinfo=timezone.utc),
        mrid=mrid,
        value_type="double",
        double_value=50.01,
    )


def _clock() -> datetime:
    return datetime(2026, 8, 19, 9, 3, tzinfo=timezone.utc)


def _timestamp(destination: Timestamp, value: datetime) -> None:
    destination.FromDatetime(value)