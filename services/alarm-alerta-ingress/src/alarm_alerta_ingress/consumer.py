"""Manual compacted Alarm replay and idempotent Alerta reconciliation."""

from __future__ import annotations

from collections.abc import Callable, Iterable
import logging
from pathlib import Path
from threading import Event
from typing import Any

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

from alarm_alerta_ingress.client import AlertaClient, AlertaClientError
from alarm_alerta_ingress.codec import AlarmDecodeError, decode_alarm, decode_alarm_key
from alarm_alerta_ingress.config import Settings
from alarm_alerta_ingress.model import DesiredAlarm, ManagedRemoteAlert
from alarm_alerta_ingress.state import AlarmRegistry


LOGGER = logging.getLogger(__name__)


class AlarmIngressError(RuntimeError):
    """Raised when complete compacted Alarm reconciliation cannot finish."""


class AlarmIngressWorker:
    """Rebuild desired Alarm state from Kafka before reconciling Alerta."""

    def __init__(
        self,
        settings: Settings,
        client: AlertaClient | Any | None = None,
        consumer_factory: Callable[..., Any] = KafkaConsumer,
    ) -> None:
        self._settings = settings
        self._client = client or AlertaClient(
            settings.alerta_url,
            settings.alerta_api_key,
            settings.alerta_request_timeout_seconds,
        )
        self._consumer_factory = consumer_factory
        self._ready_file = Path(settings.ready_file)
        self._stop = Event()

    def run(self) -> None:
        """Retry failures without relying on consumer-group offsets for recovery."""

        self._mark_not_ready()
        while not self._stop.is_set():
            consumer: Any | None = None
            try:
                consumer = self._new_consumer()
                registry = self.reconcile_initial_snapshot(consumer)
                self.reconcile_remote_snapshot(registry)
                self._mark_ready()
                self._tail(consumer, registry)
            except (AlarmDecodeError, AlarmIngressError, AlertaClientError, KafkaError, OSError) as error:
                self._mark_not_ready()
                LOGGER.warning("Alarm ingress reconciliation is unavailable; retrying: %s", error)
                self._stop.wait(self._settings.kafka_retry_interval_seconds)
            finally:
                if consumer is not None:
                    consumer.close(autocommit=False)

    def stop(self) -> None:
        """Request a prompt exit after the current Kafka poll."""

        self._stop.set()

    def reconcile_initial_snapshot(self, consumer: Any) -> AlarmRegistry:
        """Fold every partition through captured end offsets before reconciliation."""

        assignments = self._assign_partitions(consumer)
        consumer.seek_to_beginning(*assignments)
        end_offsets = consumer.end_offsets(assignments)
        registry = AlarmRegistry()
        while not self._stop.is_set() and not self._caught_up(consumer, end_offsets):
            self._apply_snapshot_records(registry, consumer.poll(timeout_ms=1_000).values())
        if self._stop.is_set():
            raise AlarmIngressError("Alarm ingress snapshot stopped")
        return registry

    def reconcile_remote_snapshot(self, registry: AlarmRegistry) -> None:
        """Bring only ingress-owned open/ack alerts in line with desired state."""

        remote_alerts = self._client.list_managed_active_or_ack()
        for remote_alert in remote_alerts:
            desired_alarm = registry.get(remote_alert.alarm_key)
            if desired_alarm is None or not _matches_desired(remote_alert, desired_alarm):
                self._close(remote_alert, "WAMA Alarm desired state cleared or superseded")
        for desired_alarm in registry.alarms:
            self._client.upsert(desired_alarm)

    def _new_consumer(self) -> Any:
        return self._consumer_factory(
            bootstrap_servers=self._settings.kafka_bootstrap_servers.split(","),
            client_id="alarm-alerta-ingress",
            enable_auto_commit=False,
            request_timeout_ms=30_000,
            api_version_auto_timeout_ms=10_000,
        )

    def _assign_partitions(self, consumer: Any) -> tuple[TopicPartition, ...]:
        partitions = consumer.partitions_for_topic(self._settings.kafka_topic)
        if not partitions:
            raise AlarmIngressError(
                f"Kafka topic {self._settings.kafka_topic!r} has no available partitions"
            )
        assignments = tuple(
            TopicPartition(self._settings.kafka_topic, partition)
            for partition in sorted(partitions)
        )
        consumer.assign(assignments)
        return assignments

    def _tail(self, consumer: Any, registry: AlarmRegistry) -> None:
        while not self._stop.is_set():
            self._apply_tail_records(registry, consumer.poll(timeout_ms=1_000).values())

    def _apply_snapshot_records(
        self,
        registry: AlarmRegistry,
        batches: Iterable[Iterable[Any]],
    ) -> None:
        for records in batches:
            for record in records:
                if record.value is None:
                    registry.remove(decode_alarm_key(record.key).alarm_key)
                else:
                    registry.upsert(decode_alarm(record.key, record.value))

    def _apply_tail_records(
        self,
        registry: AlarmRegistry,
        batches: Iterable[Iterable[Any]],
    ) -> None:
        for records in batches:
            for record in records:
                if record.value is None:
                    alarm_key = decode_alarm_key(record.key).alarm_key
                    if registry.remove(alarm_key):
                        self._reconcile_removed(alarm_key)
                    continue
                desired_alarm = decode_alarm(record.key, record.value)
                if registry.upsert(desired_alarm):
                    self._reconcile_active(desired_alarm)

    def _reconcile_active(self, desired_alarm: DesiredAlarm) -> None:
        for remote_alert in self._client.list_managed_active_or_ack():
            if remote_alert.alarm_key == desired_alarm.identity.alarm_key and not _matches_desired(
                remote_alert,
                desired_alarm,
            ):
                self._close(remote_alert, "WAMA Alarm activation superseded")
        self._client.upsert(desired_alarm)

    def _reconcile_removed(self, alarm_key: str) -> None:
        for remote_alert in self._client.list_managed_active_or_ack():
            if remote_alert.alarm_key == alarm_key:
                self._close(remote_alert, "WAMA Alarm desired state cleared")

    def _close(self, remote_alert: ManagedRemoteAlert, text: str) -> None:
        self._client.close_alert(remote_alert.alert_id, text)

    def _mark_ready(self) -> None:
        self._ready_file.parent.mkdir(parents=True, exist_ok=True)
        self._ready_file.touch()

    def _mark_not_ready(self) -> None:
        self._ready_file.unlink(missing_ok=True)

    @staticmethod
    def _caught_up(consumer: Any, end_offsets: dict[TopicPartition, int]) -> bool:
        return all(
            consumer.position(partition) >= end_offset
            for partition, end_offset in end_offsets.items()
        )


def _matches_desired(remote_alert: ManagedRemoteAlert, desired_alarm: DesiredAlarm) -> bool:
    return (
        remote_alert.event == desired_alarm.event
        and remote_alert.resource == desired_alarm.identity.mrid
    )