"""Direct serialized Kafka worker for reviewed dynamic threshold alarms."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from threading import Event
from typing import Any

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata

from processor_alarm_threshold.alarm import AlarmContractError
from processor_alarm_threshold.config import Settings
from processor_alarm_threshold.masterdata import MasterdataError
from processor_alarm_threshold.state import StateOutputs, ThresholdState


LOGGER = logging.getLogger(__name__)
_TAIL_COMMIT_BATCH_SIZE = 32


class AlarmThresholdWorkerError(RuntimeError):
    """Raised when a complete compacted snapshot cannot be acquired safely."""


@dataclass(frozen=True)
class SnapshotReplay:
    """Captured compacted-topic boundary and records fetched beyond it."""

    assignments: tuple[TopicPartition, ...]
    deferred_records: tuple[Any, ...]
    end_offsets: dict[TopicPartition, int]


class AlarmThresholdWorker:
    """Rebuild compacted state first, then serially evaluate live measurements."""

    def __init__(
        self,
        settings: Settings,
        consumer_factory: Callable[..., Any] = KafkaConsumer,
        producer_factory: Callable[..., Any] = KafkaProducer,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._settings = settings
        self._consumer_factory = consumer_factory
        self._producer_factory = producer_factory
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._stop = Event()

    def run(self) -> None:
        """Retry unavailable or unsafe snapshots without committing unsafe input."""

        while not self._stop.is_set():
            consumer: Any | None = None
            producer: Any | None = None
            try:
                consumer = self._new_consumer()
                producer = self._new_producer()
                state = ThresholdState(
                    self._settings.catalog_id,
                    self._settings.rules,
                    allow_legacy_active_alarm_bootstrap=(
                        self._settings.allow_legacy_active_alarm_bootstrap
                    ),
                )
                replay = self.reconcile_initial_snapshot(consumer, state)
                if self._stop.is_set():
                    return
                self._publish_outputs(
                    producer,
                    state.complete_initial_snapshot(self._timestamp_ms()),
                )
                self._prepare_tail_assignments(consumer, replay)
                self._tail(consumer, producer, state, replay.deferred_records)
            except (
                AlarmContractError,
                AlarmThresholdWorkerError,
                KafkaError,
                MasterdataError,
                OSError,
                ValueError,
            ) as error:
                LOGGER.warning("Alarm threshold state is not ready; retrying: %s", error)
                self._stop.wait(5)
            finally:
                if consumer is not None:
                    consumer.close(autocommit=False)
                if producer is not None:
                    producer.close(timeout=30)

    def stop(self) -> None:
        """Request a prompt exit after the active poll or producer acknowledgement."""

        self._stop.set()

    def reconcile_initial_snapshot(
        self,
        consumer: Any,
        state: ThresholdState,
    ) -> SnapshotReplay:
        """Replay compacted state through captured end offsets before tailing."""

        assignments = (
            *self._topic_partitions(consumer, self._settings.masterdata_topic),
            *self._topic_partitions(consumer, self._settings.alarm_topic),
            *self._topic_partitions(
                consumer,
                self._settings.alarm_evaluation_watermark_topic,
            ),
        )
        consumer.assign(assignments)
        consumer.seek_to_beginning(*assignments)
        end_offsets = consumer.end_offsets(assignments)
        deferred_records: list[Any] = []
        while not self._stop.is_set() and not self._caught_up(consumer, end_offsets):
            records = consumer.poll(timeout_ms=1_000)
            for partition, partition_records in records.items():
                end_offset = end_offsets.get(partition)
                if end_offset is None:
                    continue
                for record in partition_records:
                    if record.offset >= end_offset:
                        deferred_records.append(record)
                        continue
                    self._fold_snapshot_record(state, record)
        if self._stop.is_set():
            raise AlarmThresholdWorkerError("Alarm threshold snapshot replay stopped")
        return SnapshotReplay(
            assignments=assignments,
            deferred_records=tuple(deferred_records),
            end_offsets=end_offsets,
        )

    def process_tail_record(
        self,
        consumer: Any,
        producer: Any,
        state: ThresholdState,
        record: Any,
        pending_offsets: dict[TopicPartition, int] | None = None,
    ) -> bool:
        """Acknowledge one source record's outputs, then stage or commit its offset."""

        if record.topic == self._settings.masterdata_topic:
            outputs = state.apply_masterdata(record.key, record.value, self._record_timestamp(record))
        elif record.topic == self._settings.input_topic:
            outputs = state.evaluate_live_measurement(
                key=record.key,
                value=record.value,
                topic=record.topic,
                partition=record.partition,
                offset=record.offset,
                now=self._clock(),
            )
        elif record.topic == self._settings.alarm_topic:
            state.apply_alarm(record.key, record.value)
            outputs = StateOutputs()
        elif record.topic == self._settings.alarm_evaluation_watermark_topic:
            state.apply_watermark(record.key, record.value)
            outputs = StateOutputs()
        else:
            return False
        self._publish_outputs(producer, outputs)
        if pending_offsets is None:
            self._commit_record(consumer, record)
        else:
            self._stage_record_offset(pending_offsets, record)
        return True

    def _new_consumer(self) -> Any:
        return self._consumer_factory(
            bootstrap_servers=self._settings.kafka_bootstrap_servers.split(","),
            client_id="processor-alarm-threshold",
            group_id=self._settings.consumer_group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            request_timeout_ms=30_000,
            api_version_auto_timeout_ms=10_000,
        )

    def _new_producer(self) -> Any:
        return self._producer_factory(
            bootstrap_servers=self._settings.kafka_bootstrap_servers.split(","),
            client_id="processor-alarm-threshold",
            acks="all",
            retries=5,
            request_timeout_ms=30_000,
        )

    def _topic_partitions(self, consumer: Any, topic: str) -> tuple[TopicPartition, ...]:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise AlarmThresholdWorkerError(f"Kafka topic {topic!r} has no available partitions")
        return tuple(TopicPartition(topic, partition) for partition in sorted(partitions))

    @staticmethod
    def _caught_up(consumer: Any, end_offsets: dict[TopicPartition, int]) -> bool:
        return all(
            consumer.position(partition) >= end_offset
            for partition, end_offset in end_offsets.items()
        )

    def _fold_snapshot_record(self, state: ThresholdState, record: Any) -> None:
        if record.topic == self._settings.masterdata_topic:
            state.fold_masterdata_snapshot(record.key, record.value)
        elif record.topic == self._settings.alarm_topic:
            state.fold_alarm_snapshot(record.key, record.value)
        elif record.topic == self._settings.alarm_evaluation_watermark_topic:
            state.fold_watermark_snapshot(record.key, record.value)

    def _prepare_tail_assignments(self, consumer: Any, replay: SnapshotReplay) -> None:
        live_partitions = self._topic_partitions(consumer, self._settings.input_topic)
        assignments = (*replay.assignments, *live_partitions)
        consumer.assign(assignments)
        resume_offsets = dict(replay.end_offsets)
        for record in replay.deferred_records:
            partition = TopicPartition(record.topic, record.partition)
            resume_offsets[partition] = max(resume_offsets[partition], record.offset + 1)
        for partition in replay.assignments:
            consumer.seek(partition, resume_offsets[partition])
        for partition in live_partitions:
            committed = consumer.committed(partition)
            if committed is None:
                consumer.seek_to_beginning(partition)
            else:
                consumer.seek(partition, _committed_offset(committed))

    def _tail(
        self,
        consumer: Any,
        producer: Any,
        state: ThresholdState,
        deferred_records: tuple[Any, ...],
    ) -> None:
        pending_offsets: dict[TopicPartition, int] = {}
        pending_record_count = 0
        try:
            pending_record_count = self._process_tail_records(
                consumer,
                producer,
                state,
                deferred_records,
                pending_offsets,
                pending_record_count,
            )
            if self._stop.is_set():
                return
            self._commit_pending_offsets(consumer, pending_offsets)
            pending_record_count = 0
            while not self._stop.is_set():
                polled_records = consumer.poll(timeout_ms=1_000)
                if self._stop.is_set():
                    return
                for partition_records in polled_records.values():
                    pending_record_count = self._process_tail_records(
                        consumer,
                        producer,
                        state,
                        partition_records,
                        pending_offsets,
                        pending_record_count,
                    )
                    if self._stop.is_set():
                        return
                self._commit_pending_offsets(consumer, pending_offsets)
                pending_record_count = 0
        finally:
            self._commit_pending_offsets(consumer, pending_offsets)

    def _process_tail_records(
        self,
        consumer: Any,
        producer: Any,
        state: ThresholdState,
        records: Iterable[Any],
        pending_offsets: dict[TopicPartition, int],
        pending_record_count: int,
    ) -> int:
        for record in records:
            if self._stop.is_set():
                return pending_record_count
            if not self.process_tail_record(
                consumer,
                producer,
                state,
                record,
                pending_offsets,
            ):
                continue
            pending_record_count += 1
            if self._stop.is_set():
                return pending_record_count
            if pending_record_count == _TAIL_COMMIT_BATCH_SIZE:
                self._commit_pending_offsets(consumer, pending_offsets)
                pending_record_count = 0
        return pending_record_count

    def _publish_outputs(self, producer: Any, outputs: StateOutputs) -> None:
        """Acknowledge Alarm transitions before their evaluation watermarks."""

        for output in outputs.alarm_outputs:
            producer.send(
                self._settings.alarm_topic,
                key=output.key,
                value=output.value,
                timestamp_ms=output.timestamp_ms,
            ).get(timeout=30)
        for output in outputs.watermark_outputs:
            producer.send(
                self._settings.alarm_evaluation_watermark_topic,
                key=output.key,
                value=output.value,
                timestamp_ms=output.timestamp_ms,
            ).get(timeout=30)

    @staticmethod
    def _commit_record(consumer: Any, record: Any) -> None:
        AlarmThresholdWorker._commit_offsets(
            consumer,
            {TopicPartition(record.topic, record.partition): record.offset + 1},
        )

    @staticmethod
    def _stage_record_offset(pending_offsets: dict[TopicPartition, int], record: Any) -> None:
        partition = TopicPartition(record.topic, record.partition)
        pending_offsets[partition] = max(pending_offsets.get(partition, 0), record.offset + 1)

    @staticmethod
    def _commit_pending_offsets(
        consumer: Any,
        pending_offsets: dict[TopicPartition, int],
    ) -> None:
        if not pending_offsets:
            return
        offsets = dict(pending_offsets)
        pending_offsets.clear()
        AlarmThresholdWorker._commit_offsets(consumer, offsets)

    @staticmethod
    def _commit_offsets(consumer: Any, offsets: dict[TopicPartition, int]) -> None:
        consumer.commit(
            {
                partition: OffsetAndMetadata(offset, "")
                for partition, offset in offsets.items()
            }
        )

    def _record_timestamp(self, record: Any) -> int:
        timestamp = getattr(record, "timestamp", None)
        if isinstance(timestamp, int) and not isinstance(timestamp, bool) and timestamp >= 0:
            return timestamp
        return self._timestamp_ms()

    def _timestamp_ms(self) -> int:
        return int(self._clock().astimezone(timezone.utc).timestamp() * 1_000)


def _committed_offset(value: object) -> int:
    """Accept kafka-python-ng's integer and future OffsetAndMetadata forms."""

    offset = getattr(value, "offset", value)
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise AlarmThresholdWorkerError("Kafka consumer group returned an invalid committed offset")
    return offset