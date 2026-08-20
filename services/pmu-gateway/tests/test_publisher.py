"""Tests for raw-Protobuf publishing behavior."""

from __future__ import annotations

from datetime import datetime, timezone
import unittest

from pmu_gateway.config import MessageDefinition
from pmu_gateway.encoding import build_measurement
from pmu_gateway.generated import rtd_schema_pb2
from pmu_gateway.publisher import MeasurementPublisher


class FakeDelivery:
    """Successful broker acknowledgement for publisher tests."""

    def get(self, timeout: float | None = None) -> object:
        return None


class FakeProducer:
    """Captures serialized records without requiring Kafka."""

    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []

    def send(
        self,
        topic: str,
        value: bytes,
        key: bytes,
        timestamp_ms: int,
    ) -> FakeDelivery:
        self.records.append(
            {
                "topic": topic,
                "value": value,
                "key": key,
                "timestamp_ms": timestamp_ms,
            }
        )
        return FakeDelivery()


class MeasurementEncodingTests(unittest.TestCase):
    """Verify Common Format semantics independently of a running broker."""

    def test_build_measurement_sets_all_transport_timestamps(self) -> None:
        definition = MessageDefinition(
            mrid="urn:wama:poc:test:frequency",
            value_field="double_value",
            value=50.01,
            quality={"valid": True},
            field_timestamp_offset_ms=20,
        )

        measurement = build_measurement(definition, 1_700_000_000_123)

        self.assertEqual(measurement.WhichOneof("value"), "double_value")
        self.assertEqual(measurement.double_value, 50.01)
        self.assertTrue(measurement.quality.valid)
        self.assertEqual(measurement.timestamp_field.ToMilliseconds(), 1_700_000_000_103)
        self.assertEqual(measurement.timestamp_gateway.ToMilliseconds(), 1_700_000_000_123)
        self.assertEqual(measurement.timestamp_mccs.ToMilliseconds(), 1_700_000_000_123)

    def test_build_measurement_handles_timestamp_value(self) -> None:
        definition = MessageDefinition(
            mrid="urn:wama:poc:test:time",
            value_field="timestamp_value",
            value=datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc),
            quality={},
            field_timestamp_offset_ms=None,
        )

        measurement = build_measurement(definition, 1_700_000_000_123)

        self.assertEqual(measurement.WhichOneof("value"), "timestamp_value")
        self.assertEqual(
            measurement.timestamp_value.ToDatetime(tzinfo=timezone.utc),
            datetime(2026, 8, 13, 12, 0, tzinfo=timezone.utc),
        )

    def test_publisher_applies_double_value_jitter_with_injected_sampler(self) -> None:
        definition = MessageDefinition(
            mrid="urn:wama:poc:test:frequency",
            value_field="double_value",
            value=50.01,
            quality={"valid": True},
            field_timestamp_offset_ms=20,
            value_jitter=0.01,
        )
        sampled_values = iter((50.0, 50.02))
        sampler_arguments: list[tuple[float, float]] = []

        def sample_uniform(lower_bound: float, upper_bound: float) -> float:
            sampler_arguments.append((lower_bound, upper_bound))
            return next(sampled_values)

        producer = FakeProducer()
        publisher = MeasurementPublisher(
            producer,
            "LiveMeasurement",
            clock_ms=iter((1_700_000_000_123, 1_700_000_001_123)).__next__,
            random_uniform=sample_uniform,
        )

        self.assertEqual(publisher.publish_cycle((definition,)), 1)
        self.assertEqual(publisher.publish_cycle((definition,)), 1)

        messages = []
        for record in producer.records:
            message = rtd_schema_pb2.MCCSMeasurementValue()
            message.ParseFromString(record["value"])
            messages.append(message)

        self.assertEqual(len(sampler_arguments), 2)
        for lower_bound, upper_bound in sampler_arguments:
            self.assertAlmostEqual(lower_bound, 50.0)
            self.assertAlmostEqual(upper_bound, 50.02)
        self.assertEqual([message.double_value for message in messages], [50.0, 50.02])
        self.assertEqual(
            [message.timestamp_mccs.ToMilliseconds() for message in messages],
            [1_700_000_000_123, 1_700_000_001_123],
        )

    def test_publisher_preserves_zero_jitter_double_value(self) -> None:
        definition = MessageDefinition(
            mrid="urn:wama:poc:test:frequency",
            value_field="double_value",
            value=50.01,
            quality={},
            field_timestamp_offset_ms=None,
        )
        producer = FakeProducer()
        publisher = MeasurementPublisher(
            producer,
            "LiveMeasurement",
            clock_ms=lambda: 1_700_000_000_123,
            random_uniform=lambda _lower_bound, _upper_bound: self.fail(
                "zero jitter must not sample a value"
            ),
        )

        self.assertEqual(publisher.publish_cycle((definition,)), 1)

        message = rtd_schema_pb2.MCCSMeasurementValue()
        message.ParseFromString(producer.records[0]["value"])
        self.assertEqual(message.double_value, 50.01)

    def test_publisher_uses_fixture_order_and_shared_kafka_timestamp(self) -> None:
        definitions = (
            MessageDefinition(
                mrid="urn:wama:poc:test:first",
                value_field="bool_value",
                value=True,
                quality={},
                field_timestamp_offset_ms=None,
            ),
            MessageDefinition(
                mrid="urn:wama:poc:test:second",
                value_field="int_value",
                value=3,
                quality={},
                field_timestamp_offset_ms=None,
            ),
        )
        producer = FakeProducer()
        publisher = MeasurementPublisher(
            producer,
            "LiveMeasurement",
            clock_ms=lambda: 1_700_000_000_123,
        )

        self.assertEqual(publisher.publish_cycle(definitions), 2)

        decoded = []
        for record in producer.records:
            message = rtd_schema_pb2.MCCSMeasurementValue()
            message.ParseFromString(record["value"])
            decoded.append(message)
            self.assertEqual(record["timestamp_ms"], 1_700_000_000_123)
            self.assertEqual(
                message.timestamp_mccs.ToMilliseconds(),
                record["timestamp_ms"],
            )

        self.assertEqual([message.mrid for message in decoded], [item.mrid for item in definitions])
        self.assertEqual(decoded[0].WhichOneof("value"), "bool_value")
        self.assertEqual(decoded[1].WhichOneof("value"), "int_value")


if __name__ == "__main__":
    unittest.main()