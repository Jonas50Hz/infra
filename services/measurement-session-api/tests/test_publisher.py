"""Kafka wire publication behavior for session requests."""

from __future__ import annotations

from datetime import datetime, timezone
import unittest

from measurement_session_common.generated.measurement_session_pb2 import MeasurementSessionRequest
from measurement_session_api.config import Settings
from measurement_session_api.publisher import KafkaSessionPublisher


class _Future:
    def __init__(self) -> None:
        self.timeout: int | None = None

    def get(self, timeout: int) -> None:
        self.timeout = timeout


class _Producer:
    def __init__(self) -> None:
        self.calls = []
        self.closed_timeout: int | None = None
        self.future = _Future()

    def send(self, topic, **kwargs):
        self.calls.append((topic, kwargs))
        return self.future

    def close(self, timeout: int) -> None:
        self.closed_timeout = timeout


class KafkaSessionPublisherTests(unittest.TestCase):
    """Use a keyed deterministic Protobuf payload and request-time timestamp."""

    def test_publish_uses_contract_identity_and_timestamp(self) -> None:
        producer = _Producer()
        settings = Settings(
            kafka_bootstrap_servers="kafka:9092,kafka-2:9092",
            kafka_topic="MeasurementSession",
            max_interval_hours=24,
            max_mrids=32,
            publish_timeout_seconds=7,
            grafana_session_dashboard_url="http://localhost:3001/d/session",
        )
        publisher = KafkaSessionPublisher(settings, producer_factory=lambda **_: producer)
        request = MeasurementSessionRequest(session_id="d70ba792-70a6-42bd-8d15-7c75c4f5079d")
        request.requested_at.FromDatetime(datetime(2026, 8, 21, 12, 0, 1, 250000, tzinfo=timezone.utc))

        publisher.publish(request)
        publisher.close()

        self.assertEqual(len(producer.calls), 1)
        topic, arguments = producer.calls[0]
        self.assertEqual(topic, "MeasurementSession")
        self.assertEqual(arguments["key"], request.session_id.encode("utf-8"))
        self.assertEqual(arguments["value"], request.SerializeToString(deterministic=True))
        self.assertEqual(arguments["timestamp_ms"], 1_787_313_601_250)
        self.assertEqual(producer.future.timeout, 7)
        self.assertEqual(producer.closed_timeout, 7)


if __name__ == "__main__":
    unittest.main()