"""Tests for phase-isolated fake-PMU apparent-power calculations."""

from __future__ import annotations

import unittest

from processor_apparent_power.generated.rtd_schema_pb2 import MCCSMeasurementValue
from processor_apparent_power.pipeline import (
    OUTPUT_MRIDS,
    PhaseCache,
    output_key,
)


class PipelineTests(unittest.TestCase):
    """Ensure only complete, valid phase pairs produce apparent power."""

    def test_calculates_apparent_power_after_voltage_then_current(self) -> None:
        cache = PhaseCache()
        voltage = self._measurement("voltage-l1", 230.4)
        current = self._measurement("current-l1", 318.2)
        source_bytes = current.SerializeToString()

        self.assertIsNone(cache.transform(voltage, self._key(voltage), 1_000))
        derived = cache.transform(current, self._key(current), 2_000)

        self.assertIsNotNone(derived)
        assert derived is not None
        self.assertEqual(derived.measurement.mrid, OUTPUT_MRIDS["l1"])
        self.assertAlmostEqual(derived.measurement.double_value, 73_313.28)
        self.assertEqual(derived.kafka_timestamp_ms, 2_000)
        self.assertEqual(output_key(derived.measurement), OUTPUT_MRIDS["l1"].encode())
        self.assertEqual(derived.measurement.timestamp_field, current.timestamp_field)
        self.assertEqual(derived.measurement.timestamp_gateway, current.timestamp_gateway)
        self.assertEqual(derived.measurement.timestamp_mccs, current.timestamp_mccs)
        self.assertEqual(derived.measurement.quality, current.quality)
        self.assertEqual(current.SerializeToString(), source_bytes)

    def test_calculates_apparent_power_after_current_then_voltage(self) -> None:
        cache = PhaseCache()
        current = self._measurement("current-l2", 316.7)
        voltage = self._measurement("voltage-l2", 229.8)

        self.assertIsNone(cache.transform(current, self._key(current), 1_000))
        derived = cache.transform(voltage, self._key(voltage), 2_000)

        self.assertIsNotNone(derived)
        assert derived is not None
        self.assertEqual(derived.measurement.mrid, OUTPUT_MRIDS["l2"])
        self.assertAlmostEqual(derived.measurement.double_value, 72_777.66)

    def test_keeps_phase_measurements_isolated(self) -> None:
        cache = PhaseCache()
        voltage_l1 = self._measurement("voltage-l1", 230.4)
        current_l2 = self._measurement("current-l2", 316.7)
        current_l1 = self._measurement("current-l1", 318.2)

        self.assertIsNone(cache.transform(voltage_l1, self._key(voltage_l1), 1_000))
        self.assertIsNone(cache.transform(current_l2, self._key(current_l2), 2_000))
        derived = cache.transform(current_l1, self._key(current_l1), 3_000)

        self.assertIsNotNone(derived)
        assert derived is not None
        self.assertEqual(derived.measurement.mrid, OUTPUT_MRIDS["l1"])
        self.assertAlmostEqual(derived.measurement.double_value, 73_313.28)

    def test_rejects_invalid_and_non_source_records_without_updating_cache(self) -> None:
        cache = PhaseCache()
        voltage = self._measurement("voltage-l3", 230.1)
        invalid_current = self._measurement("current-l3", 317.4, valid=False)
        missing_quality_current = MCCSMeasurementValue(
            mrid=f"{self._source_prefix()}:current-l3",
            double_value=317.4,
        )
        non_double_current = self._measurement("current-l3", 0.0)
        non_double_current.ClearField("double_value")
        non_double_current.int_value = 317
        unrelated_frequency = self._measurement("frequency", 50.01)
        own_output = MCCSMeasurementValue(
            mrid=OUTPUT_MRIDS["l3"],
            double_value=73_033.74,
        )

        self.assertIsNone(cache.transform(voltage, b"unexpected-key", 1_000))
        self.assertIsNone(cache.transform(invalid_current, self._key(invalid_current), 2_000))
        self.assertIsNone(
            cache.transform(missing_quality_current, self._key(missing_quality_current), 3_000)
        )
        self.assertIsNone(
            cache.transform(non_double_current, self._key(non_double_current), 4_000)
        )
        self.assertIsNone(
            cache.transform(unrelated_frequency, self._key(unrelated_frequency), 5_000)
        )
        self.assertIsNone(cache.transform(own_output, self._key(own_output), 6_000))
        self.assertIsNone(cache.transform(voltage, self._key(voltage), 7_000))

        valid_current = self._measurement("current-l3", 317.4)
        derived = cache.transform(valid_current, self._key(valid_current), 8_000)
        self.assertIsNotNone(derived)
        assert derived is not None
        self.assertAlmostEqual(derived.measurement.double_value, 73_033.74)

    def test_replayed_valid_source_produces_deterministic_output(self) -> None:
        cache = PhaseCache()
        voltage = self._measurement("voltage-l1", 230.4)
        current = self._measurement("current-l1", 318.2)

        self.assertIsNone(cache.transform(voltage, self._key(voltage), 1_000))
        first = cache.transform(current, self._key(current), 2_000)
        second = cache.transform(current, self._key(current), 2_000)

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        assert first is not None
        assert second is not None
        self.assertEqual(first.kafka_timestamp_ms, second.kafka_timestamp_ms)
        self.assertEqual(
            first.measurement.SerializeToString(),
            second.measurement.SerializeToString(),
        )

    def _measurement(
        self,
        source_name: str,
        value: float,
        valid: bool = True,
    ) -> MCCSMeasurementValue:
        measurement = MCCSMeasurementValue(
            mrid=f"{self._source_prefix()}:{source_name}",
            double_value=value,
        )
        measurement.timestamp_field.seconds = 1_726_000_123
        measurement.timestamp_field.nanos = 436_000_000
        measurement.timestamp_gateway.seconds = 1_726_000_123
        measurement.timestamp_gateway.nanos = 446_000_000
        measurement.timestamp_mccs.seconds = 1_726_000_123
        measurement.timestamp_mccs.nanos = 456_000_000
        measurement.quality.valid = valid
        measurement.quality.substituted = False
        return measurement

    def _key(self, measurement: MCCSMeasurementValue) -> bytes:
        return measurement.mrid.encode("utf-8")

    def _source_prefix(self) -> str:
        return "urn:wama:poc:pmu:bay-01"