"""Kafka publication for validated raw-Protobuf session requests."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

from kafka import KafkaProducer
from kafka.errors import KafkaError

from measurement_session_common.generated.measurement_session_pb2 import MeasurementSessionRequest
from measurement_session_api.config import Settings


class SessionPublishError(RuntimeError):
    """Raised when a validated request cannot be acknowledged by Kafka."""


class SessionPublisher(Protocol):
    """Publish one request and release resources during service shutdown."""

    def publish(self, request: MeasurementSessionRequest) -> None:
        """Publish a raw-Protobuf request under its immutable session key."""

    def close(self) -> None:
        """Close the underlying publisher resources."""


class KafkaSessionPublisher:
    """Synchronous Kafka publisher with an acknowledgement before HTTP success."""

    def __init__(
        self,
        settings: Settings,
        producer_factory: Callable[..., Any] = KafkaProducer,
    ) -> None:
        self._topic = settings.kafka_topic
        self._timeout_seconds = settings.publish_timeout_seconds
        self._producer = producer_factory(
            bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
            acks="all",
            retries=5,
            request_timeout_ms=30_000,
        )

    def publish(self, request: MeasurementSessionRequest) -> None:
        """Wait for Kafka acknowledgement of the deterministic wire payload."""

        timestamp_milliseconds = (
            request.requested_at.seconds * 1_000 + request.requested_at.nanos // 1_000_000
        )
        try:
            future = self._producer.send(
                self._topic,
                key=request.session_id.encode("utf-8"),
                value=request.SerializeToString(deterministic=True),
                timestamp_ms=timestamp_milliseconds,
            )
            future.get(timeout=self._timeout_seconds)
        except (KafkaError, OSError, TimeoutError) as error:
            raise SessionPublishError(f"MeasurementSession publication failed: {error}") from error

    def close(self) -> None:
        """Flush acknowledged work before the HTTP service stops."""

        self._producer.close(timeout=self._timeout_seconds)