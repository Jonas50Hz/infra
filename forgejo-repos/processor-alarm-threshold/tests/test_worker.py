"""Mocked direct Kafka worker tests for acknowledgement, replay, and offsets."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
import unittest

from kafka import TopicPartition

from processor_alarm_threshold.alarm import AlarmContractError
from processor_alarm_threshold.config import Settings
from processor_alarm_threshold.state import StateOutputs, ThresholdState
from processor_alarm_threshold.worker import AlarmThresholdWorker, SnapshotReplay

from support import CATALOG_ID, FREQUENCY_MRID, measurement, reviewed_rules, source


NOW = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)


class WorkerOrderingTests(unittest.TestCase):
    """Outputs must be acknowledged before source offsets advance."""

    def test_writes_and_acknowledges_alarm_then_watermark_before_committing_live_offset(self) -> None:
        events: list[str] = []
        consumer = _CommitConsumer(events)
        producer = _Producer(events)
        worker = self._worker()
        state = self._member_state()
        record = _live_record(offset=7)

        worker.process_tail_record(consumer, producer, state, record)

        self.assertEqual(events, ["send", "ack", "send", "ack", "commit"])
        self.assertEqual(
            [output["topic"] for output in producer.outputs],
            ["Alarm", "AlarmEvaluationWatermark"],
        )
        self.assertEqual(consumer.commits[0][TopicPartition("LiveMeasurement", 0)].offset, 8)

    def test_failed_ack_does_not_commit_and_replay_reuses_the_deterministic_episode(self) -> None:
        events: list[str] = []
        consumer = _CommitConsumer(events)
        failing_producer = _Producer(events, fail_ack=True)
        worker = self._worker()
        record = _live_record(offset=9)

        with self.assertRaisesRegex(RuntimeError, "acknowledgement failed"):
            worker.process_tail_record(consumer, failing_producer, self._member_state(), record)

        replay_producer = _Producer([])
        worker.process_tail_record(_CommitConsumer([]), replay_producer, self._member_state(), record)

        self.assertEqual(events, ["send", "ack"])
        self.assertEqual(consumer.commits, [])
        self.assertEqual(failing_producer.outputs[0]["value"], replay_producer.outputs[0]["value"])
        self.assertEqual(
            [output["topic"] for output in replay_producer.outputs],
            ["Alarm", "AlarmEvaluationWatermark"],
        )

    def test_inactive_qualifying_measurement_writes_only_a_watermark(self) -> None:
        events: list[str] = []
        consumer = _CommitConsumer(events)
        producer = _Producer(events)
        record = _record(
            "LiveMeasurement",
            FREQUENCY_MRID.encode("utf-8"),
            measurement(value=49.0, observed_at=NOW),
            7,
        )

        self._worker().process_tail_record(consumer, producer, self._member_state(), record)

        self.assertEqual(events, ["send", "ack", "commit"])
        self.assertEqual([output["topic"] for output in producer.outputs], ["AlarmEvaluationWatermark"])

    def test_replay_after_watermark_acknowledgement_does_not_duplicate_alarm_transition(self) -> None:
        events: list[str] = []
        consumer = _CommitConsumer(events, fail_commit=True)
        producer = _Producer(events)
        worker = self._worker()
        record = _live_record(offset=10)

        with self.assertRaisesRegex(RuntimeError, "offset commit failed"):
            worker.process_tail_record(consumer, producer, self._member_state(), record)

        restored = self._member_state()
        for output in producer.outputs:
            if output["topic"] == "Alarm":
                restored.fold_alarm_snapshot(output["key"], output["value"])
            else:
                restored.fold_watermark_snapshot(output["key"], output["value"])
        self.assertEqual(restored.complete_initial_snapshot(0), StateOutputs())

        replay_events: list[str] = []
        replay_producer = _Producer(replay_events)
        replay_consumer = _CommitConsumer(replay_events)
        worker.process_tail_record(replay_consumer, replay_producer, restored, record)

        self.assertEqual(events, ["send", "ack", "send", "ack", "commit"])
        self.assertEqual(replay_events, ["commit"])
        self.assertEqual(replay_producer.outputs, [])

    def test_alarm_acknowledgement_without_watermark_leaves_recovery_unready(self) -> None:
        events: list[str] = []
        producer = _Producer(events, fail_ack_topic="AlarmEvaluationWatermark")
        record = _live_record(offset=10)

        with self.assertRaisesRegex(RuntimeError, "acknowledgement failed"):
            self._worker().process_tail_record(
                _CommitConsumer(events),
                producer,
                self._member_state(),
                record,
            )

        restored = self._member_state()
        restored.fold_alarm_snapshot(producer.outputs[0]["key"], producer.outputs[0]["value"])
        with self.assertRaisesRegex(AlarmContractError, "no AlarmEvaluationWatermark"):
            restored.complete_initial_snapshot(0)

    def test_replays_compacted_topics_through_captured_end_offsets_before_tailing(self) -> None:
        seed_state = self._member_state()
        activation = seed_state.evaluate_live_measurement(
            key=FREQUENCY_MRID.encode("utf-8"),
            value=measurement(value=50.2, observed_at=NOW),
            topic="LiveMeasurement",
            partition=0,
            offset=7,
            now=NOW,
        )
        masterdata_partition = TopicPartition("Masterdata", 0)
        alarm_partition = TopicPartition("Alarm", 0)
        watermark_partition = TopicPartition("AlarmEvaluationWatermark", 0)
        consumer = _SnapshotConsumer(
            {
                masterdata_partition: [
                    _record("Masterdata", b"pmu-bay-01", source("pmu-bay-01"), 0),
                ],
                alarm_partition: [
                    _record(
                        "Alarm",
                        activation.alarm_outputs[0].key,
                        activation.alarm_outputs[0].value,
                        0,
                    ),
                ],
                watermark_partition: [
                    _record(
                        "AlarmEvaluationWatermark",
                        activation.watermark_outputs[0].key,
                        activation.watermark_outputs[0].value,
                        0,
                    ),
                ],
            }
        )
        worker = self._worker()
        state = ThresholdState(CATALOG_ID, reviewed_rules())

        replay = worker.reconcile_initial_snapshot(consumer, state)

        self.assertEqual(state.member_mrids, frozenset({FREQUENCY_MRID}))
        self.assertEqual(state.complete_initial_snapshot(0), StateOutputs())
        self.assertEqual(state.active_alarm_keys, seed_state.active_alarm_keys)
        self.assertEqual(replay.end_offsets[masterdata_partition], 1)
        self.assertEqual(replay.end_offsets[alarm_partition], 1)
        self.assertEqual(replay.end_offsets[watermark_partition], 1)

    def test_replayed_watermark_suppresses_an_older_live_alarm_transition(self) -> None:
        seed_state = self._member_state()
        cleared = seed_state.evaluate_live_measurement(
            key=FREQUENCY_MRID.encode("utf-8"),
            value=measurement(value=49.0, observed_at=NOW + timedelta(seconds=10)),
            topic="LiveMeasurement",
            partition=0,
            offset=10,
            now=NOW,
        )
        masterdata_partition = TopicPartition("Masterdata", 0)
        alarm_partition = TopicPartition("Alarm", 0)
        watermark_partition = TopicPartition("AlarmEvaluationWatermark", 0)
        consumer = _SnapshotConsumer(
            {
                masterdata_partition: [
                    _record("Masterdata", b"pmu-bay-01", source("pmu-bay-01"), 0),
                ],
                alarm_partition: [],
                watermark_partition: [
                    _record(
                        "AlarmEvaluationWatermark",
                        cleared.watermark_outputs[0].key,
                        cleared.watermark_outputs[0].value,
                        0,
                    ),
                ],
            }
        )
        worker = self._worker()
        restored = ThresholdState(CATALOG_ID, reviewed_rules())

        worker.reconcile_initial_snapshot(consumer, restored)
        self.assertEqual(restored.complete_initial_snapshot(0), StateOutputs())
        events: list[str] = []
        worker.process_tail_record(
            _CommitConsumer(events),
            _Producer(events),
            restored,
            _record(
                "LiveMeasurement",
                FREQUENCY_MRID.encode("utf-8"),
                measurement(value=50.2, observed_at=NOW + timedelta(seconds=5)),
                11,
            ),
        )

        self.assertEqual(events, ["commit"])

    def test_stopped_tail_does_not_process_deferred_snapshot_records(self) -> None:
        masterdata_partition = TopicPartition("Masterdata", 0)
        alarm_partition = TopicPartition("Alarm", 0)
        watermark_partition = TopicPartition("AlarmEvaluationWatermark", 0)
        live_partition = TopicPartition("LiveMeasurement", 0)
        consumer = _SnapshotConsumer(
            {
                masterdata_partition: [
                    _record("Masterdata", b"pmu-bay-01", source("pmu-bay-01"), 0),
                    _record("Masterdata", b"pmu-bay-01", None, 1),
                ],
                alarm_partition: [],
                watermark_partition: [],
                live_partition: [],
            },
            end_offsets={
                masterdata_partition: 1,
                alarm_partition: 0,
                watermark_partition: 0,
            },
        )
        worker = self._worker()
        state = ThresholdState(CATALOG_ID, reviewed_rules())

        replay = worker.reconcile_initial_snapshot(consumer, state)
        self.assertEqual(state.member_mrids, frozenset({FREQUENCY_MRID}))
        self.assertEqual(state.complete_initial_snapshot(0), StateOutputs())
        worker._prepare_tail_assignments(consumer, replay)
        worker.stop()
        producer = _Producer([])
        worker._tail(consumer, producer, state, replay.deferred_records)

        self.assertEqual(state.member_mrids, frozenset({FREQUENCY_MRID}))
        self.assertEqual(producer.outputs, [])
        self.assertEqual(consumer.commits, [])

    def test_tail_batches_acknowledged_offsets_across_partitions_and_topics(self) -> None:
        events: list[str] = []
        worker = self._worker()
        producer = _Producer(events)
        consumer = _TailConsumer(
            events,
            [
                {
                    TopicPartition("LiveMeasurement", 0): [
                        _live_record(
                            offset,
                            partition=0,
                            observed_at=NOW + timedelta(milliseconds=offset),
                        )
                        for offset in range(17)
                    ],
                    TopicPartition("LiveMeasurement", 1): [
                        _live_record(
                            offset,
                            partition=1,
                            observed_at=NOW + timedelta(milliseconds=17 + offset),
                        )
                        for offset in range(16)
                    ],
                    TopicPartition("Masterdata", 0): [
                        _record("Masterdata", b"pmu-bay-01", source("pmu-bay-01"), 0),
                    ],
                }
            ],
            worker.stop,
        )

        worker._tail(consumer, producer, self._member_state(), ())

        self.assertEqual(
            [output["topic"] for output in producer.outputs],
            ["Alarm", "AlarmEvaluationWatermark"] * 33,
        )
        self.assertEqual(
            events,
            ["send", "ack", "send", "ack"] * 32
            + ["commit"]
            + ["send", "ack", "send", "ack", "commit"],
        )
        self.assertEqual(len(consumer.commits), 2)
        self.assertEqual(
            consumer.commits[0][TopicPartition("LiveMeasurement", 0)].offset,
            17,
        )
        self.assertEqual(
            consumer.commits[0][TopicPartition("LiveMeasurement", 1)].offset,
            15,
        )
        self.assertEqual(
            consumer.commits[1][TopicPartition("LiveMeasurement", 1)].offset,
            16,
        )
        self.assertEqual(
            consumer.commits[1][TopicPartition("Masterdata", 0)].offset,
            1,
        )

    def test_tail_stops_after_acknowledged_record_when_stop_arrives_during_output_acknowledgement(
        self,
    ) -> None:
        events: list[str] = []
        worker = self._worker()
        producer = _StopOnWatermarkAcknowledgementProducer(events, worker.stop)
        consumer = _TailConsumer(
            events,
            [
                {
                    TopicPartition("LiveMeasurement", 0): [
                        _live_record(7),
                        _live_record(8, observed_at=NOW + timedelta(seconds=1)),
                    ]
                }
            ],
            worker.stop,
        )

        worker._tail(consumer, producer, self._member_state(), ())

        self.assertEqual(events, ["send", "ack", "send", "ack", "commit"])
        self.assertEqual(
            [output["topic"] for output in producer.outputs],
            ["Alarm", "AlarmEvaluationWatermark"],
        )
        self.assertEqual(len(consumer.commits), 1)
        self.assertEqual(set(consumer.commits[0]), {TopicPartition("LiveMeasurement", 0)})
        self.assertEqual(
            consumer.commits[0][TopicPartition("LiveMeasurement", 0)].offset,
            8,
        )

    def test_tail_does_not_admit_a_record_yielded_after_stop(self) -> None:
        events: list[str] = []
        worker = self._worker()
        consumer = _CommitConsumer(events)
        producer = _Producer(events)

        def records() -> Iterable[SimpleNamespace]:
            yield _live_record(7)
            worker.stop()
            yield _live_record(8, observed_at=NOW + timedelta(seconds=1))

        pending_offsets: dict[TopicPartition, int] = {}
        pending_record_count = worker._process_tail_records(
            consumer,
            producer,
            self._member_state(),
            records(),
            pending_offsets,
            0,
        )

        self.assertEqual(events, ["send", "ack", "send", "ack"])
        self.assertEqual(
            [output["topic"] for output in producer.outputs],
            ["Alarm", "AlarmEvaluationWatermark"],
        )
        self.assertEqual(pending_record_count, 1)
        self.assertEqual(pending_offsets, {TopicPartition("LiveMeasurement", 0): 8})
        worker._commit_pending_offsets(consumer, pending_offsets)
        self.assertEqual(len(consumer.commits), 1)
        self.assertEqual(
            consumer.commits[0][TopicPartition("LiveMeasurement", 0)].offset,
            8,
        )

    def test_run_stops_after_successful_replay_before_recovery_output(self) -> None:
        seed_state = self._member_state()
        activation = seed_state.evaluate_live_measurement(
            key=FREQUENCY_MRID.encode("utf-8"),
            value=measurement(value=50.2, observed_at=NOW),
            topic="LiveMeasurement",
            partition=0,
            offset=7,
            now=NOW,
        )
        masterdata_partition = TopicPartition("Masterdata", 0)
        alarm_partition = TopicPartition("Alarm", 0)
        watermark_partition = TopicPartition("AlarmEvaluationWatermark", 0)
        live_partition = TopicPartition("LiveMeasurement", 0)
        consumer = _SnapshotConsumer(
            {
                masterdata_partition: [
                    _record("Masterdata", b"pmu-bay-01", source("pmu-bay-01"), 0),
                ],
                alarm_partition: [
                    _record(
                        "Alarm",
                        activation.alarm_outputs[0].key,
                        activation.alarm_outputs[0].value,
                        0,
                    ),
                ],
                watermark_partition: [],
                live_partition: [],
            }
        )
        events: list[str] = []
        producer = _Producer(events)
        worker = _StopAfterSuccessfulSnapshotWorker(
            Settings(
                alarm_topic="Alarm",
                catalog_id=CATALOG_ID,
                config_path="/etc/wama/alarm-threshold.yaml",
                consumer_group="processor-alarm-threshold",
                input_topic="LiveMeasurement",
                kafka_bootstrap_servers="kafka:9092",
                masterdata_topic="Masterdata",
                rules=reviewed_rules(),
                alarm_evaluation_watermark_topic="AlarmEvaluationWatermark",
                allow_legacy_active_alarm_bootstrap=True,
            ),
            consumer_factory=lambda **factory_options: consumer,
            producer_factory=lambda **factory_options: producer,
            clock=lambda: NOW,
        )

        worker.run()

        self.assertTrue(worker.snapshot_replayed)
        self.assertEqual(events, [])
        self.assertEqual(producer.outputs, [])
        self.assertEqual(consumer.commits, [])

    @staticmethod
    def _worker() -> AlarmThresholdWorker:
        return AlarmThresholdWorker(
            Settings(
                alarm_topic="Alarm",
                catalog_id=CATALOG_ID,
                config_path="/etc/wama/alarm-threshold.yaml",
                consumer_group="processor-alarm-threshold",
                input_topic="LiveMeasurement",
                kafka_bootstrap_servers="kafka:9092",
                masterdata_topic="Masterdata",
                rules=reviewed_rules(),
                alarm_evaluation_watermark_topic="AlarmEvaluationWatermark",
            ),
            clock=lambda: NOW,
        )

    @staticmethod
    def _member_state() -> ThresholdState:
        state = ThresholdState(CATALOG_ID, reviewed_rules())
        state.fold_masterdata_snapshot(b"pmu-bay-01", source("pmu-bay-01"))
        state.complete_initial_snapshot(0)
        return state


def _live_record(
    offset: int,
    *,
    partition: int = 0,
    observed_at: datetime = NOW,
) -> SimpleNamespace:
    return _record(
        "LiveMeasurement",
        FREQUENCY_MRID.encode("utf-8"),
        measurement(value=50.2, observed_at=observed_at),
        offset,
        partition=partition,
    )


def _record(
    topic: str,
    key: bytes,
    value: bytes | None,
    offset: int,
    *,
    partition: int = 0,
) -> SimpleNamespace:
    return SimpleNamespace(
        key=key,
        value=value,
        topic=topic,
        partition=partition,
        offset=offset,
        timestamp=int(NOW.timestamp() * 1_000),
    )


class _Producer:
    def __init__(
        self,
        events: list[str],
        fail_ack: bool = False,
        fail_ack_topic: str | None = None,
    ) -> None:
        self._events = events
        self._fail_ack = fail_ack
        self._fail_ack_topic = fail_ack_topic
        self.outputs: list[dict[str, object]] = []

    def send(self, topic: str, **kwargs: object) -> "_Future":
        self._events.append("send")
        self.outputs.append({"topic": topic, **kwargs})
        return _Future(self._events, self._fail_ack or topic == self._fail_ack_topic)

    def close(self, timeout: int) -> None:
        pass


class _StopOnWatermarkAcknowledgementProducer(_Producer):
    def __init__(self, events: list[str], stop: Callable[[], None]) -> None:
        super().__init__(events)
        self._stop = stop

    def send(self, topic: str, **kwargs: object) -> "_Future":
        future = super().send(topic, **kwargs)
        if topic == "AlarmEvaluationWatermark":
            return _StopAfterAcknowledgementFuture(self._events, self._stop)
        return future


class _Future:
    def __init__(self, events: list[str], fail_ack: bool) -> None:
        self._events = events
        self._fail_ack = fail_ack

    def get(self, timeout: int) -> None:
        self._events.append("ack")
        if self._fail_ack:
            raise RuntimeError("acknowledgement failed")


class _StopAfterAcknowledgementFuture(_Future):
    def __init__(self, events: list[str], stop: Callable[[], None]) -> None:
        super().__init__(events, False)
        self._stop = stop

    def get(self, timeout: int) -> None:
        super().get(timeout)
        self._stop()


class _CommitConsumer:
    def __init__(self, events: list[str], fail_commit: bool = False) -> None:
        self._events = events
        self._fail_commit = fail_commit
        self.commits: list[dict[TopicPartition, object]] = []

    def commit(self, offsets: dict[TopicPartition, object]) -> None:
        self._events.append("commit")
        self.commits.append(offsets)
        if self._fail_commit:
            raise RuntimeError("offset commit failed")


class _TailConsumer(_CommitConsumer):
    def __init__(
        self,
        events: list[str],
        poll_batches: list[dict[TopicPartition, list[SimpleNamespace]]],
        stop: Callable[[], None],
    ) -> None:
        super().__init__(events)
        self._poll_batches = iter(poll_batches)
        self._stop = stop

    def poll(self, timeout_ms: int) -> dict[TopicPartition, list[SimpleNamespace]]:
        try:
            return next(self._poll_batches)
        except StopIteration:
            self._stop()
            return {}


class _SnapshotConsumer:
    def __init__(
        self,
        records: dict[TopicPartition, list[SimpleNamespace]],
        *,
        end_offsets: dict[TopicPartition, int] | None = None,
    ) -> None:
        self._records = records
        self._positions = {partition: 0 for partition in records}
        self._end_offsets = end_offsets or {
            partition: len(partition_records) for partition, partition_records in records.items()
        }
        self.assignments: tuple[TopicPartition, ...] = ()
        self.commits: list[dict[TopicPartition, object]] = []
        self._polled = False

    def partitions_for_topic(self, topic: str) -> set[int]:
        return {partition.partition for partition in self._records if partition.topic == topic}

    def assign(self, assignments: tuple[TopicPartition, ...]) -> None:
        self.assignments = assignments

    def seek_to_beginning(self, *assignments: TopicPartition) -> None:
        for assignment in assignments:
            self._positions[assignment] = 0

    def end_offsets(self, assignments: tuple[TopicPartition, ...]) -> dict[TopicPartition, int]:
        return {assignment: self._end_offsets[assignment] for assignment in assignments}

    def position(self, partition: TopicPartition) -> int:
        return self._positions[partition]

    def seek(self, partition: TopicPartition, offset: int) -> None:
        self._positions[partition] = offset

    @staticmethod
    def committed(partition: TopicPartition) -> None:
        return None

    def commit(self, offsets: dict[TopicPartition, object]) -> None:
        self.commits.append(offsets)

    def close(self, autocommit: bool) -> None:
        pass

    def poll(self, timeout_ms: int) -> dict[TopicPartition, list[SimpleNamespace]]:
        if self._polled:
            return {}
        self._polled = True
        for partition, records in self._records.items():
            self._positions[partition] = len(records)
        return self._records


class _StopAfterSuccessfulSnapshotWorker(AlarmThresholdWorker):
    snapshot_replayed = False

    def reconcile_initial_snapshot(
        self,
        consumer: Any,
        state: ThresholdState,
    ) -> SnapshotReplay:
        replay = super().reconcile_initial_snapshot(consumer, state)
        self.snapshot_replayed = True
        self.stop()
        return replay


if __name__ == "__main__":
    unittest.main()