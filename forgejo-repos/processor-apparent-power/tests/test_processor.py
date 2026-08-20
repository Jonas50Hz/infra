"""Tests for the apparent-power calculation an EE normally edits."""

from __future__ import annotations

import unittest

from wama_processor import InputMeasurement, ProcessorDefinition
from wama_processor.testing import input_measurement

from processor_apparent_power.processor import PhaseCache, build_processor


class ProcessorTests(unittest.TestCase):
    """Ensure complete, valid phase pairs produce apparent power."""

    def test_calculates_apparent_power_after_voltage_then_current(self) -> None:
        cache, definition = self._processor()
        voltage = self._input(definition, "voltage_l1", 230.4, timestamp=1_000)
        current = self._input(definition, "current_l1", 318.2, timestamp=2_000)

        self.assertIsNone(cache.transform(voltage))
        derived = cache.transform(current)

        self.assertIsNotNone(derived)
        assert derived is not None
        self.assertEqual(derived.name, "apparent_power_l1")
        self.assertAlmostEqual(derived.double_value, 73_313.28)
        self.assertEqual(derived.kafka_timestamp_ms, 2_000)

    def test_calculates_apparent_power_after_current_then_voltage(self) -> None:
        cache, definition = self._processor()
        current = self._input(definition, "current_l2", 316.7)
        voltage = self._input(definition, "voltage_l2", 229.8)

        self.assertIsNone(cache.transform(current))
        derived = cache.transform(voltage)

        self.assertIsNotNone(derived)
        assert derived is not None
        self.assertEqual(derived.name, "apparent_power_l2")
        self.assertAlmostEqual(derived.double_value, 72_777.66)

    def test_keeps_phase_measurements_isolated(self) -> None:
        cache, definition = self._processor()
        voltage_l1 = self._input(definition, "voltage_l1", 230.4)
        current_l2 = self._input(definition, "current_l2", 316.7)
        current_l1 = self._input(definition, "current_l1", 318.2)

        self.assertIsNone(cache.transform(voltage_l1))
        self.assertIsNone(cache.transform(current_l2))
        derived = cache.transform(current_l1)

        self.assertIsNotNone(derived)
        assert derived is not None
        self.assertEqual(derived.name, "apparent_power_l1")
        self.assertAlmostEqual(derived.double_value, 73_313.28)

    def test_ignores_invalid_and_non_numeric_inputs_without_updating_cache(self) -> None:
        cache, definition = self._processor()
        voltage = self._input(definition, "voltage_l3", 230.1)
        invalid_current = self._input(
            definition,
            "current_l3",
            317.4,
            is_valid=False,
        )
        non_numeric_current = self._input(definition, "current_l3", None)

        self.assertIsNone(cache.transform(voltage))
        self.assertIsNone(cache.transform(invalid_current))
        self.assertIsNone(cache.transform(non_numeric_current))

        valid_current = self._input(definition, "current_l3", 317.4)
        derived = cache.transform(valid_current)

        self.assertIsNotNone(derived)
        assert derived is not None
        self.assertAlmostEqual(derived.double_value, 73_033.74)

    def test_replayed_valid_source_produces_deterministic_output(self) -> None:
        cache, definition = self._processor()
        voltage = self._input(definition, "voltage_l1", 230.4, timestamp=1_000)
        current = self._input(definition, "current_l1", 318.2, timestamp=2_000)

        self.assertIsNone(cache.transform(voltage))
        first = cache.transform(current)
        second = cache.transform(current)

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        assert first is not None
        assert second is not None
        self.assertEqual(first.kafka_timestamp_ms, second.kafka_timestamp_ms)
        self.assertEqual(first.protobuf.SerializeToString(), second.protobuf.SerializeToString())

    def _processor(self) -> tuple[PhaseCache, ProcessorDefinition]:
        cache = PhaseCache()
        return cache, build_processor(cache)

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