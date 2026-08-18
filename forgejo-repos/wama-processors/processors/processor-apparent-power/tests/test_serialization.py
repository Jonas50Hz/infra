"""Tests for raw-Protobuf apparent-power serialization."""

from __future__ import annotations

import unittest

from processor_apparent_power.generated.rtd_schema_pb2 import MCCSMeasurementValue
from processor_apparent_power.pipeline import OUTPUT_MRIDS, PhaseCache


class ProtobufSerializationTests(unittest.TestCase):
    """Keep derived apparent power on the raw Common Format contract."""

    def test_round_trips_a_derived_apparent_power_measurement(self) -> None:
        cache = PhaseCache()
        voltage = self._measurement("voltage-l1", 230.4)
        current = self._measurement("current-l1", 318.2)
        self.assertIsNone(cache.transform(voltage, voltage.mrid.encode(), 1_000))
        derived = cache.transform(current, current.mrid.encode(), 2_000)

        self.assertIsNotNone(derived)
        assert derived is not None
        encoded = derived.measurement.SerializeToString()
        decoded = MCCSMeasurementValue()
        decoded.ParseFromString(encoded)

        self.assertEqual(decoded.mrid, OUTPUT_MRIDS["l1"])
        self.assertEqual(decoded.WhichOneof("value"), "double_value")
        self.assertAlmostEqual(decoded.double_value, 73_313.28)
        self.assertTrue(decoded.quality.valid)

    def _measurement(self, source_name: str, value: float) -> MCCSMeasurementValue:
        measurement = MCCSMeasurementValue(
            mrid=f"urn:wama:poc:pmu:bay-01:{source_name}",
            double_value=value,
        )
        measurement.quality.valid = True
        return measurement