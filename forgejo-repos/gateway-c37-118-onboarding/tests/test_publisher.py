"""Tests for one-shot Kafka publication semantics."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
import unittest

from kafka import TopicPartition

from gateway_c37_118_onboarding.publisher import (
    Settings,
    publish_plan,
    read_compacted_state,
)
from gateway_c37_118_onboarding.reconciliation import PublishRecord, ReconciliationPlan


class PublisherTests(unittest.TestCase):
    """Require acknowledged source upserts and null-valued tombstones."""

    def test_publishes_upserts_and_tombstones_with_one_timestamp(self) -> None:
        producer = _Producer()
        plan = ReconciliationPlan(
            upserts=(PublishRecord("pmu-bay-01", b"source"),),
            tombstones=("pmu-bay-02",),
        )
        published_at = datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc)

        count = publish_plan(producer, "Masterdata", plan, published_at)

        self.assertEqual(count, 2)
        self.assertEqual(
            producer.records,
            [
                ("Masterdata", b"pmu-bay-01", b"source", int(published_at.timestamp() * 1_000)),
                ("Masterdata", b"pmu-bay-02", None, int(published_at.timestamp() * 1_000)),
            ],
        )

    def test_reads_latest_compacted_state_including_tombstones(self) -> None:
        consumer = _Consumer()

        state = read_compacted_state(consumer, "Masterdata")

        self.assertEqual(state, {"pmu-bay-01": None, "pmu-bay-02": b"active"})

    def test_uses_expected_local_defaults(self) -> None:
        settings = Settings.from_environment({})

        self.assertEqual(settings.topic, "Masterdata")
        self.assertEqual(settings.catalog_id, "wama-c37-118-onboarding")
        self.assertEqual(settings.catalog_revision, "development")


class _Delivery:
    def get(self, timeout: float | None = None) -> object:
        return object()


class _Producer:
    def __init__(self) -> None:
        self.records: list[tuple[str, bytes, bytes | None, int]] = []

    def send(
        self,
        topic: str,
        key: bytes,
        value: bytes | None,
        timestamp_ms: int,
    ) -> _Delivery:
        self.records.append((topic, key, value, timestamp_ms))
        return _Delivery()


class _Consumer:
    def __init__(self) -> None:
        self._partition = TopicPartition("Masterdata", 0)
        self._position = 0

    def partitions_for_topic(self, topic: str) -> set[int]:
        return {0}

    def assign(self, assignments: set[TopicPartition]) -> None:
        self._partition = next(iter(assignments))

    def seek_to_beginning(self, *assignments: TopicPartition) -> None:
        self._position = 0

    def end_offsets(self, assignments: set[TopicPartition]) -> dict[TopicPartition, int]:
        return {self._partition: 3}

    def position(self, partition: TopicPartition) -> int:
        return self._position

    def poll(self, timeout_ms: int) -> dict[TopicPartition, list[SimpleNamespace]]:
        self._position = 3
        return {
            self._partition: [
                SimpleNamespace(key=b"pmu-bay-01", value=b"active"),
                SimpleNamespace(key=b"pmu-bay-02", value=b"active"),
                SimpleNamespace(key=b"pmu-bay-01", value=None),
            ]
        }