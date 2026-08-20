"""Tests for framework-owned apparent-power Common Format preservation."""

from __future__ import annotations

import unittest

from wama_processor.generated.rtd_schema_pb2 import MCCSMeasurementValue

from processor_apparent_power.processor import INPUTS, OUTPUTS, build_processor


class ProtobufSerializationTests(unittest.TestCase):
    """Keep derived apparent power on the raw Common Format contract."""

    def test_round_trips_a_derived_apparent_power_measurement(self) -> None:
        definition = build_processor()
        voltage = MCCSMeasurementValue(
            mrid=INPUTS["voltage_l1"],
            double_value=230.4,
        )
        current = MCCSMeasurementValue(
            mrid=INPUTS["current_l1"],
            double_value=318.2,
        )
        voltage.quality.valid = True
        current.quality.valid = True
        current.timestamp_mccs.seconds = 1_726_000_123
        source_bytes = current.SerializeToString()

        self.assertIsNone(
            definition.transform_record(voltage, voltage.mrid.encode(), 1_000)
        )
        derived = definition.transform_record(current, current.mrid.encode(), 2_000)

        self.assertIsNotNone(derived)
        assert derived is not None
        encoded = derived.protobuf.SerializeToString()
        decoded = MCCSMeasurementValue()
        decoded.ParseFromString(encoded)

        self.assertEqual(decoded.mrid, OUTPUTS["apparent_power_l1"])
        self.assertEqual(decoded.WhichOneof("value"), "double_value")
        self.assertAlmostEqual(decoded.double_value, 73_313.28)
        self.assertTrue(decoded.quality.valid)
        self.assertEqual(decoded.timestamp_mccs, current.timestamp_mccs)
        self.assertEqual(current.SerializeToString(), source_bytes)

    def test_rejects_wrong_keys_unrelated_records_and_own_outputs(self) -> None:
        definition = build_processor()
        voltage = MCCSMeasurementValue(
            mrid=INPUTS["voltage_l1"],
            double_value=230.4,
        )
        unrelated = MCCSMeasurementValue(
            mrid="urn:wama:poc:pmu:bay-01:frequency",
            double_value=50.01,
        )
        own_output = MCCSMeasurementValue(
            mrid=OUTPUTS["apparent_power_l1"],
            double_value=73_313.28,
        )

        self.assertIsNone(definition.transform_record(voltage, b"wrong-key", 1_000))
        self.assertIsNone(
            definition.transform_record(unrelated, unrelated.mrid.encode(), 2_000)
        )
        self.assertIsNone(
            definition.transform_record(own_output, own_output.mrid.encode(), 3_000)
        )