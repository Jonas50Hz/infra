"""Manual-assignment Kafka reconciliation for generated gateway dashboards."""

from __future__ import annotations

import logging
from pathlib import Path
from threading import Event
from collections.abc import Callable, Iterable
from typing import Any

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from gateway_dashboard_provisioner.codec import MasterdataDecodeError, decode_source, decode_source_id
from gateway_dashboard_provisioner.config import Settings
from gateway_dashboard_provisioner.state import GatewayRegistry
from gateway_dashboard_provisioner.storage import DashboardStore


LOGGER = logging.getLogger(__name__)


class GatewayDashboardConsumerError(RuntimeError):
    """Raised when a complete compacted Masterdata snapshot cannot be acquired."""


class MasterdataDashboardWorker:
    """Rebuild and tail active gateway dashboard state without Kafka offsets."""

    def __init__(
        self,
        settings: Settings,
        store: DashboardStore,
        consumer_factory: Callable[..., Any] = KafkaConsumer,
    ) -> None:
        self._settings = settings
        self._store = store
        self._consumer_factory = consumer_factory
        self._ready_file = Path(settings.ready_file)
        self._stop = Event()

    def run(self) -> None:
        """Retry broker faults while keeping the previous complete dashboard snapshot."""

        self._mark_not_ready()
        while not self._stop.is_set():
            consumer: Any | None = None
            try:
                consumer = self._new_consumer()
                registry = self.reconcile_initial_snapshot(consumer)
                self._store.publish(registry.sources)
                self._mark_ready()
                self._tail(consumer, registry)
            except (GatewayDashboardConsumerError, KafkaError, MasterdataDecodeError, OSError) as error:
                self._mark_not_ready()
                LOGGER.warning("Gateway dashboard reconciliation is unavailable; retrying: %s", error)
                self._stop.wait(self._settings.kafka_retry_interval_seconds)
            finally:
                if consumer is not None:
                    consumer.close(autocommit=False)

    def stop(self) -> None:
        """Request a prompt exit after the current Kafka poll."""

        self._stop.set()

    def reconcile_initial_snapshot(self, consumer: Any) -> GatewayRegistry:
        """Fold each partition through its captured end offset before publishing."""

        assignments = self._assign_partitions(consumer)
        consumer.seek_to_beginning(*assignments)
        end_offsets = consumer.end_offsets(assignments)
        registry = GatewayRegistry()
        while not self._stop.is_set() and not self._caught_up(consumer, end_offsets):
            self._apply_records(registry, consumer.poll(timeout_ms=1_000).values())
        if self._stop.is_set():
            raise GatewayDashboardConsumerError("Gateway dashboard reconciliation stopped")
        return registry

    def _new_consumer(self) -> Any:
        return self._consumer_factory(
            bootstrap_servers=self._settings.kafka_bootstrap_servers.split(","),
            client_id="gateway-dashboard-provisioner",
            enable_auto_commit=False,
            request_timeout_ms=30_000,
            api_version_auto_timeout_ms=10_000,
        )

    def _assign_partitions(self, consumer: Any) -> tuple[TopicPartition, ...]:
        partitions = consumer.partitions_for_topic(self._settings.kafka_topic)
        if not partitions:
            raise GatewayDashboardConsumerError(
                f"Kafka topic {self._settings.kafka_topic!r} has no available partitions"
            )
        assignments = tuple(
            TopicPartition(self._settings.kafka_topic, partition)
            for partition in sorted(partitions)
        )
        consumer.assign(assignments)
        return assignments

    @staticmethod
    def _caught_up(consumer: Any, end_offsets: dict[TopicPartition, int]) -> bool:
        return all(consumer.position(partition) >= end_offset for partition, end_offset in end_offsets.items())

    def _tail(self, consumer: Any, registry: GatewayRegistry) -> None:
        while not self._stop.is_set():
            if self._apply_records(registry, consumer.poll(timeout_ms=1_000).values()):
                self._store.publish(registry.sources)

    @staticmethod
    def _apply_records(
        registry: GatewayRegistry,
        batches: Iterable[Iterable[Any]],
    ) -> bool:
        changed = False
        for records in batches:
            for record in records:
                if record.value is None:
                    changed = registry.remove(decode_source_id(record.key)) or changed
                else:
                    changed = registry.upsert(decode_source(record.key, record.value)) or changed
        return changed

    def _mark_ready(self) -> None:
        self._ready_file.parent.mkdir(parents=True, exist_ok=True)
        self._ready_file.touch()

    def _mark_not_ready(self) -> None:
        self._ready_file.unlink(missing_ok=True)