"""Kafka publishing behavior for the fake PMU gateway."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from time import time_ns
from typing import Protocol

from pmu_gateway.config import MessageDefinition
from pmu_gateway.encoding import build_measurement


class DeliveryFuture(Protocol):
    """Minimal Kafka delivery interface used by the gateway."""

    def get(self, timeout: float | None = None) -> object:
        """Wait for broker acknowledgement."""


class KafkaProducer(Protocol):
    """Minimal producer interface used by the gateway."""

    def send(
        self,
        topic: str,
        value: bytes,
        key: bytes,
        timestamp_ms: int,
    ) -> DeliveryFuture:
        """Send one raw Kafka record."""


class MeasurementPublisher:
    """Publishes a configured message batch at one synchronized instant."""

    def __init__(
        self,
        producer: KafkaProducer,
        topic: str,
        clock_ms: Callable[[], int] = lambda: time_ns() // 1_000_000,
        delivery_timeout_seconds: float = 10.0,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._clock_ms = clock_ms
        self._delivery_timeout_seconds = delivery_timeout_seconds

    def publish_cycle(self, definitions: Iterable[MessageDefinition]) -> int:
        """Publish all configured records in fixture order and await delivery."""

        publish_timestamp_ms = self._clock_ms()
        count = 0
        for definition in definitions:
            message = build_measurement(definition, publish_timestamp_ms)
            delivery = self._producer.send(
                self._topic,
                key=definition.mrid.encode("utf-8"),
                value=message.SerializeToString(),
                timestamp_ms=publish_timestamp_ms,
            )
            delivery.get(timeout=self._delivery_timeout_seconds)
            count += 1
        return count