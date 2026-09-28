"""Tests for the deadline-driven LFR Kafka adapter."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from quixstreams.models.messages import KafkaMessage

from processor_lfr_frequency_provision.classification import CountThresholds, VoltageThresholds
from processor_lfr_frequency_provision.config import LfrConfig, PmuConfig
from processor_lfr_frequency_provision.engine import LfrSecondEngine
from processor_lfr_frequency_provision.generated import rtd_schema_pb2
from processor_lfr_frequency_provision.runtime import (
    RuntimeConfigurationError,
    RuntimeSettings,
    _poll_timeout_seconds,
    run_loop,
)
from processor_lfr_frequency_provision.selection import EvenMedianTieBreak
from processor_lfr_frequency_provision.state import StateStore


class RuntimeTests(unittest.TestCase):
    """Require idle-time closure and at-least-once output publication behavior."""

    def test_closes_idle_second_publishes_output_and_commits_after_state_persistence(self) -> None:
        timestamp_ms = 1_726_000_123_000
        clock = _Clock(timestamp_ms)
        source_records = [
            self._record("urn:wama:test:pmu-a:frequency", 50.01, timestamp_ms),
            self._record("urn:wama:test:pmu-a:voltage", 230.0, timestamp_ms),
        ]
        consumer = _Consumer(source_records, clock)
        producer = _Producer()
        input_topic = _InputTopic()
        output_topic = _OutputTopic()
        engine = LfrSecondEngine(self._config())

        with TemporaryDirectory() as directory:
            state_store = StateStore(Path(directory) / "state.sqlite3")
            running = iter((True, True, True, True, False))
            run_loop(
                config=self._config(),
                consumer=consumer,
                engine=engine,
                input_topic=input_topic,
                output_topic=output_topic,
                poll_interval_ms=50,
                producer=producer,
                state_store=state_store,
                now_ms=clock,
                keep_running=lambda: next(running),
            )

            self.assertEqual(len(consumer.committed), 2)
            self.assertEqual(len(producer.produced), 1)
            output = producer.produced[0]
            self.assertEqual(output["key"], b"urn:wama:test:lfr:preferred-frequency")
            decoded = rtd_schema_pb2.MCCSMeasurementValue()
            decoded.ParseFromString(output["value"])
            self.assertEqual(decoded.mrid, "urn:wama:test:lfr:preferred-frequency")
            self.assertAlmostEqual(decoded.double_value, 50.01)
            self.assertEqual(state_store.pending_publications(), ())
            state_store.close()

    def test_uses_a_short_poll_to_reach_the_next_close_deadline(self) -> None:
        self.assertEqual(_poll_timeout_seconds(1_500, 600, 100), 0.1)
        self.assertEqual(_poll_timeout_seconds(1_590, 600, 100), 0.01)
        self.assertEqual(_poll_timeout_seconds(1_600, 600, 100), 0.1)

    def test_rejects_invalid_runtime_poll_interval(self) -> None:
        with patch.dict("os.environ", {"LFR_POLL_INTERVAL_MS": "101"}, clear=True):
            with self.assertRaisesRegex(RuntimeConfigurationError, "LFR_POLL_INTERVAL_MS"):
                RuntimeSettings.from_environment()

    def _config(self) -> LfrConfig:
        return LfrConfig(
            close_delay_ms=600,
            even_median_tie_break=EvenMedianTieBreak.LOWER_FREQUENCY,
            frequency_min_hz=49.0,
            frequency_max_hz=51.0,
            maximum_future_seconds=1,
            output_mrid="urn:wama:test:lfr:preferred-frequency",
            pmus=(
                PmuConfig(
                    pmu_id="pmu-a",
                    frequency_mrid="urn:wama:test:pmu-a:frequency",
                    voltage_mrid="urn:wama:test:pmu-a:voltage",
                    nominal_voltage=230.0,
                    count_thresholds=CountThresholds(0, 1),
                    voltage_thresholds=VoltageThresholds(1.0, 2.0),
                ),
            ),
            status_evidence_mode="generic_quality_provisional",
        )

    def _record(self, mrid: str, value: float, timestamp_ms: int) -> rtd_schema_pb2.MCCSMeasurementValue:
        record = rtd_schema_pb2.MCCSMeasurementValue(mrid=mrid, double_value=value)
        record.timestamp_field.seconds = timestamp_ms // 1_000
        record.timestamp_field.nanos = (timestamp_ms % 1_000) * 1_000_000
        record.quality.valid = True
        return record


class _Clock:
    def __init__(self, current_ms: int) -> None:
        self.current_ms = current_ms

    def __call__(self) -> int:
        return self.current_ms


class _Consumer:
    def __init__(self, records: list[rtd_schema_pb2.MCCSMeasurementValue], clock: _Clock) -> None:
        self._clock = clock
        self._records = records
        self._offset = 0
        self.committed: list[_RawMessage] = []
        self.subscriptions: list[list[str]] = []
        self.closed = False

    def subscribe(self, topics: list[str]) -> None:
        self.subscriptions.append(topics)

    def poll(self, timeout: float):
        if self._offset < len(self._records):
            record = self._records[self._offset]
            raw = _RawMessage(record, self._offset)
            self._offset += 1
            return raw
        self._clock.current_ms += 1_600
        return None

    def commit(self, message: "_RawMessage", asynchronous: bool) -> None:
        self.committed.append(message)
        if asynchronous:
            raise AssertionError("LFR must synchronously commit only after durable state")

    def close(self) -> None:
        self.closed = True


@dataclass
class _RawMessage:
    value: rtd_schema_pb2.MCCSMeasurementValue
    raw_offset: int

    def error(self):
        return None

    def offset(self) -> int:
        return self.raw_offset

    def partition(self) -> int:
        return 0

    def topic(self) -> str:
        return "LiveMeasurement"


class _InputTopic:
    name = "LiveMeasurement"

    def deserialize(self, raw: _RawMessage) -> KafkaMessage:
        return KafkaMessage(
            key=raw.value.mrid.encode("utf-8"),
            value=raw.value,
            headers=[],
            timestamp=raw.value.timestamp_field.ToMilliseconds(),
        )


class _OutputTopic:
    name = "LiveMeasurement"

    def serialize(self, key: bytes, value: rtd_schema_pb2.MCCSMeasurementValue, timestamp_ms: int) -> KafkaMessage:
        return KafkaMessage(
            key=key,
            value=value.SerializeToString(),
            headers=[],
            timestamp=timestamp_ms,
        )


class _Producer:
    def __init__(self) -> None:
        self.produced: list[dict[str, object]] = []

    def produce(self, topic: str, **kwargs: object) -> None:
        self.produced.append({"topic": topic, **kwargs})
        callback = kwargs["on_delivery"]
        callback(None, object())

    def flush(self, timeout: int) -> int:
        return 0


if __name__ == "__main__":
    unittest.main()