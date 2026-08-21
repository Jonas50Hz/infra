"""Tests for the apparent-power calculation an EE normally edits."""

from __future__ import annotations

import unittest

from wama_processor import InputMeasurement, ProcessorDefinition
from wama_processor.testing import input_measurement

from processor_apparent_power.processor import build_processor


class ProcessorTests(unittest.TestCase):
    """Ensure complete, valid phase pairs produce apparent power."""

    def test_calculates_apparent_power_after_voltage_then_current(self) -> None:
        definition = self._processor()
        voltage = self._input(definition, "voltage_l1", 230.4, timestamp=1_000)
        current = self._input(definition, "current_l1", 318.2, timestamp=2_000)

        self.assertIsNone(definition.transform(voltage))
        derived = definition.transform(current)

        self.assertIsNotNone(derived)
        assert derived is not None
        self.assertEqual(derived.name, "apparent_power_l1")
        self.assertAlmostEqual(derived.double_value, 73_313.28)
        self.assertEqual(derived.kafka_timestamp_ms, 2_000)

    def test_calculates_apparent_power_after_current_then_voltage(self) -> None:
        definition = self._processor()
        current = self._input(definition, "current_l2", 316.7)
        voltage = self._input(definition, "voltage_l2", 229.8)

        self.assertIsNone(definition.transform(current))
        derived = definition.transform(voltage)

        self.assertIsNotNone(derived)
        assert derived is not None
        self.assertEqual(derived.name, "apparent_power_l2")
        self.assertAlmostEqual(derived.double_value, 72_777.66)

    def test_keeps_phase_measurements_isolated(self) -> None:
        definition = self._processor()
        voltage_l1 = self._input(definition, "voltage_l1", 230.4)
        current_l2 = self._input(definition, "current_l2", 316.7)
        current_l1 = self._input(definition, "current_l1", 318.2)

        self.assertIsNone(definition.transform(voltage_l1))
        self.assertIsNone(definition.transform(current_l2))
        derived = definition.transform(current_l1)

        self.assertIsNotNone(derived)
        assert derived is not None
        self.assertEqual(derived.name, "apparent_power_l1")
        self.assertAlmostEqual(derived.double_value, 73_313.28)

    def test_ignores_invalid_and_non_numeric_inputs_without_updating_cache(self) -> None:
        definition = self._processor()
        voltage = self._input(definition, "voltage_l3", 230.1)
        invalid_current = self._input(
            definition,
            "current_l3",
            317.4,
            is_valid=False,
        )
        non_numeric_current = self._input(definition, "current_l3", None)

        self.assertIsNone(definition.transform(voltage))
        self.assertIsNone(definition.transform(invalid_current))
        self.assertIsNone(definition.transform(non_numeric_current))

        valid_current = self._input(definition, "current_l3", 317.4)
        derived = definition.transform(valid_current)

        self.assertIsNotNone(derived)
        assert derived is not None
        self.assertAlmostEqual(derived.double_value, 73_033.74)

    def test_replayed_valid_source_produces_deterministic_output(self) -> None:
        definition = self._processor()
        voltage = self._input(definition, "voltage_l1", 230.4, timestamp=1_000)
        current = self._input(definition, "current_l1", 318.2, timestamp=2_000)

        self.assertIsNone(definition.transform(voltage))
        first = definition.transform(current)
        second = definition.transform(current)

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        assert first is not None
        assert second is not None
        self.assertEqual(first.kafka_timestamp_ms, second.kafka_timestamp_ms)
        self.assertEqual(first.protobuf.SerializeToString(), second.protobuf.SerializeToString())

    def test_expires_stale_values_and_drops_cache_after_restart(self) -> None:
        definition = self._processor()
        voltage = self._input(definition, "voltage_l1", 230.4, timestamp=1_000)
        stale_current = self._input(definition, "current_l1", 318.2, timestamp=3_001)
        fresh_voltage = self._input(definition, "voltage_l1", 230.4, timestamp=3_002)

        self.assertIsNone(definition.transform(voltage))
        self.assertIsNone(definition.transform(stale_current))
        self.assertIsNotNone(definition.transform(fresh_voltage))

        restarted = self._processor()
        self.assertIsNone(restarted.transform(stale_current))

    def _processor(self) -> ProcessorDefinition:
        return build_processor()

    def _input(
        self,
        definition: ProcessorDefinition,
        name: str,
        double_value: float | None,
        *,
        is_valid: bool = True,
        timestamp: int = 0,
    ) -> InputMeasurement:
        return input_measurement(
            definition,
            name,
            double_value,
            is_valid=is_valid,
            kafka_timestamp_ms=timestamp,
        )