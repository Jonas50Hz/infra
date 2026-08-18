"""Tests for Kafka key alignment before immutable catalog insertion."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
import unittest

from google.protobuf.timestamp_pb2 import Timestamp
from kafka.errors import NoBrokersAvailable

from measurement_session_common.contract import DEFAULT_SESSION_BUCKET, MANIFEST_MEDIA_TYPE
from measurement_session_common.generated.measurement_session_pb2 import MeasurementSession
from measurement_session_api.catalog import CatalogError
from measurement_session_api.consumer import CatalogWorker


class _Catalog:
    def __init__(self) -> None:
        self.sessions = []

    def insert(self, session) -> None:
        self.sessions.append(session)


class CatalogWorkerTests(unittest.TestCase):
    """Ensure a mismatched Kafka key never reaches the catalog."""

    def test_accepts_matching_raw_protobuf_key(self) -> None:
        catalog = _Catalog()
        worker = CatalogWorker(SimpleNamespace(), catalog)
        message = _message()
        record = SimpleNamespace(key=message.session_id.encode("utf-8"), value=message.SerializeToString())

        worker.process_record(record)

        self.assertEqual(len(catalog.sessions), 1)

    def test_rejects_mismatched_key(self) -> None:
        worker = CatalogWorker(SimpleNamespace(), _Catalog())
        message = _message()
        record = SimpleNamespace(key=b"unexpected", value=message.SerializeToString())

        with self.assertRaisesRegex(CatalogError, "Kafka key"):
            worker.process_record(record)

    def test_retries_broker_connection_and_becomes_ready(self) -> None:
        settings = SimpleNamespace(
            kafka_topic="MeasurementSession",
            kafka_bootstrap_servers="kafka:9092",
            kafka_consumer_group="measurement-session-catalog-api",
        )
        attempts = []
        worker: CatalogWorker

        def consumer_factory(*args, **kwargs):
            del args, kwargs
            attempts.append("connect")
            if len(attempts) == 1:
                raise NoBrokersAvailable()
            return _StoppingConsumer(worker)

        worker = CatalogWorker(
            settings,
            _Catalog(),
            consumer_factory=consumer_factory,
            retry_interval_seconds=0.001,
        )

        worker._run()

        self.assertEqual(attempts, ["connect", "connect"])
        self.assertTrue(worker.ready)
        self.assertIsNone(worker.failure)


def _message() -> MeasurementSession:
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
    message.manifest.bucket = DEFAULT_SESSION_BUCKET
    message.manifest.object_key = f"sessions/{message.session_id}/manifest.json"
    message.manifest.byte_length = 100
    message.manifest.media_type = MANIFEST_MEDIA_TYPE
    message.manifest.sha256 = b"x" * 32
    return message


class _StoppingConsumer:
    def __init__(self, worker: CatalogWorker) -> None:
        self._worker = worker

    def poll(self, timeout_ms: int):
        del timeout_ms
        self._worker._stop.set()
        return {}

    def close(self, autocommit: bool) -> None:
        del autocommit