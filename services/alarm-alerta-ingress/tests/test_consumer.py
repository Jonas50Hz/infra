"""Tests for manual snapshot replay and marker-scoped reconciliation."""

from __future__ import annotations

from types import SimpleNamespace
import unittest

from kafka import TopicPartition

from alarm_alerta_ingress.codec import decode_alarm
from alarm_alerta_ingress.config import Settings
from alarm_alerta_ingress.consumer import AlarmIngressWorker
from alarm_alerta_ingress.model import ManagedRemoteAlert
from alarm_alerta_ingress.state import AlarmRegistry
from test_codec import _alarm_record


class AlarmIngressWorkerTests(unittest.TestCase):
    """Require a complete end-offset snapshot before Alerta reconciliation."""

    def test_snapshot_folds_tombstone_without_consumer_group_offsets(self) -> None:
        key, payload = _alarm_record()
        partition = TopicPartition("Alarm", 0)
        consumer = _Consumer(
            partition,
            [
                SimpleNamespace(key=key, value=payload),
                SimpleNamespace(key=key, value=None),
            ],
        )
        worker = AlarmIngressWorker(_settings(), client=_Client())

        registry = worker.reconcile_initial_snapshot(consumer)

        self.assertEqual(registry.alarms, ())
        self.assertEqual(consumer.assignments, (partition,))

    def test_snapshot_closes_only_stale_managed_alerts_then_upserts_desired_state(self) -> None:
        key, payload = _alarm_record()
        desired_alarm = decode_alarm(key, payload)
        registry = AlarmRegistry()
        registry.upsert(desired_alarm)
        client = _Client(
            remote_alerts=(
                ManagedRemoteAlert(
                    alert_id="matching",
                    alarm_key=desired_alarm.identity.alarm_key,
                    event=desired_alarm.event,
                    resource=desired_alarm.identity.mrid,
                    status="ack",
                ),
                ManagedRemoteAlert(
                    alert_id="stale",
                    alarm_key="alarm/v1/stale/stale",
                    event="wama-alarm/stale",
                    resource="urn:wama:poc:stale",
                    status="open",
                ),
            )
        )
        worker = AlarmIngressWorker(_settings(), client=client)

        worker.reconcile_remote_snapshot(registry)

        self.assertEqual(client.closed, [("stale", "WAMA Alarm desired state cleared or superseded")])
        self.assertEqual(client.upserted, [desired_alarm])

    def test_new_consumer_does_not_configure_a_group_id(self) -> None:
        created: list[dict[str, object]] = []

        def factory(**kwargs: object) -> _Consumer:
            created.append(kwargs)
            return _Consumer(TopicPartition("Alarm", 0), [])

        worker = AlarmIngressWorker(_settings(), client=_Client(), consumer_factory=factory)
        worker._new_consumer()

        self.assertNotIn("group_id", created[0])
        self.assertFalse(created[0]["enable_auto_commit"])


def _settings() -> Settings:
    return Settings(
        alerta_api_key="test-key",
        alerta_request_timeout_seconds=10,
        alerta_url="http://alerta:8080",
        kafka_bootstrap_servers="kafka:9092",
        kafka_retry_interval_seconds=1,
        kafka_topic="Alarm",
        ready_file="/tmp/alarm-alerta-ingress-test-ready",
    )


class _Client:
    def __init__(self, remote_alerts: tuple[ManagedRemoteAlert, ...] = ()) -> None:
        self.remote_alerts = remote_alerts
        self.closed: list[tuple[str, str]] = []
        self.upserted: list[object] = []

    def close_alert(self, alert_id: str, text: str) -> None:
        self.closed.append((alert_id, text))

    def list_managed_active_or_ack(self) -> tuple[ManagedRemoteAlert, ...]:
        return self.remote_alerts

    def upsert(self, alarm: object) -> None:
        self.upserted.append(alarm)


class _Consumer:
    def __init__(self, partition: TopicPartition, records: list[SimpleNamespace]) -> None:
        self._partition = partition
        self._records = records
        self._position = 0
        self.assignments: tuple[TopicPartition, ...] = ()

    def assign(self, assignments: tuple[TopicPartition, ...]) -> None:
        self.assignments = assignments

    def end_offsets(self, assignments: tuple[TopicPartition, ...]) -> dict[TopicPartition, int]:
        return {self._partition: len(self._records)}

    def partitions_for_topic(self, topic: str) -> set[int]:
        return {self._partition.partition}

    def poll(self, timeout_ms: int) -> dict[TopicPartition, list[SimpleNamespace]]:
        if self._position:
            return {}
        self._position = len(self._records)
        return {self._partition: self._records}

    def position(self, partition: TopicPartition) -> int:
        return self._position

    def seek_to_beginning(self, *assignments: TopicPartition) -> None:
        self._position = 0