"""Tests for the stateless fake-PMU frequency transformation."""

from __future__ import annotations

import unittest

from processor_frequency_scale.generated.rtd_schema_pb2 import MCCSMeasurementValue
from processor_frequency_scale.pipeline import (
    OUTPUT_KEY,
    OUTPUT_MRID,
    SOURCE_KEY,
    SOURCE_MRID,
    output_key,
    transform,
)


class PipelineTests(unittest.TestCase):
    """Prove that only the PMU frequency produces a deterministic output."""

    def test_scales_frequency_and_preserves_common_format_context(self) -> None:
        measurement = self._source_measurement()
        source_bytes = measurement.SerializeToString()

        derived = transform(measurement, SOURCE_KEY, 1_726_000_123_456)

        self.assertIsNotNone(derived)
        assert derived is not None
        self.assertEqual(derived.measurement.mrid, OUTPUT_MRID)
        self.assertEqual(derived.measurement.WhichOneof("value"), "double_value")
        self.assertEqual(derived.measurement.double_value, 50_010.0)
        self.assertEqual(derived.measurement.timestamp_field, measurement.timestamp_field)
        self.assertEqual(derived.measurement.timestamp_gateway, measurement.timestamp_gateway)
        self.assertEqual(derived.measurement.timestamp_mccs, measurement.timestamp_mccs)
        self.assertEqual(derived.measurement.quality, measurement.quality)
        self.assertEqual(derived.kafka_timestamp_ms, 1_726_000_123_456)
        self.assertEqual(output_key(derived.measurement), OUTPUT_KEY)
        self.assertEqual(measurement.SerializeToString(), source_bytes)

    def test_rejects_every_non_source_record(self) -> None:
        measurement = MCCSMeasurementValue(
            mrid=SOURCE_MRID,
            double_value=50.0,
        )
        wrong_mrid = MCCSMeasurementValue(
            mrid="urn:wama:poc:pmu:bay-01:voltage-l1",
            double_value=230.4,
        )
        non_double_source = MCCSMeasurementValue(mrid=SOURCE_MRID, int_value=50)
        own_output = transform(measurement, SOURCE_KEY, 1)

        self.assertIsNotNone(own_output)
        assert own_output is not None
        for candidate, key in (
            (measurement, b"unexpected-key"),
            (wrong_mrid, SOURCE_KEY),
            (non_double_source, SOURCE_KEY),
            (own_output.measurement, OUTPUT_KEY),
        ):
            with self.subTest(mrid=candidate.mrid, key=key):
                self.assertIsNone(transform(candidate, key, 1))

    def test_replayed_source_produces_identical_output(self) -> None:
        measurement = self._source_measurement()

        first = transform(measurement, SOURCE_KEY, 1_726_000_123_456)
        second = transform(measurement, SOURCE_KEY, 1_726_000_123_456)

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        assert first is not None
        assert second is not None
        self.assertEqual(first.kafka_timestamp_ms, second.kafka_timestamp_ms)
        self.assertEqual(
            first.measurement.SerializeToString(),
            second.measurement.SerializeToString(),
        )

    def _source_measurement(self) -> MCCSMeasurementValue:
        measurement = MCCSMeasurementValue(mrid=SOURCE_MRID, double_value=50.01)
        measurement.timestamp_field.seconds = 1_726_000_123
        measurement.timestamp_field.nanos = 436_000_000
        measurement.timestamp_gateway.seconds = 1_726_000_123
        measurement.timestamp_gateway.nanos = 446_000_000
        measurement.timestamp_mccs.seconds = 1_726_000_123
        measurement.timestamp_mccs.nanos = 456_000_000
        measurement.quality.valid = True
        measurement.quality.substituted = False
        return measurement