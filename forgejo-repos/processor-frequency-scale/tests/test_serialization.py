"""Tests for framework-owned Common Format preservation."""

from __future__ import annotations

import unittest

from wama_processor.generated.rtd_schema_pb2 import MCCSMeasurementValue

from processor_frequency_scale.processor import (
    FREQUENCY_HZ,
    FREQUENCY_MILLIHERTZ,
    INPUTS,
    OUTPUTS,
    PROCESSOR,
)


class ProtobufSerializationTests(unittest.TestCase):
    """Keep the derived value on the raw Common Format contract."""

    def test_round_trips_a_derived_common_format_measurement(self) -> None:
        original = MCCSMeasurementValue(
            mrid=INPUTS[FREQUENCY_HZ],
            double_value=50.01,
        )
        original.quality.valid = True
        original.timestamp_mccs.seconds = 1_726_000_123
        derived = PROCESSOR.transform_record(
            original,
            INPUTS[FREQUENCY_HZ].encode("utf-8"),
            1_726_000_123_000,
        )

        self.assertIsNotNone(derived)
        assert derived is not None
        encoded = derived.protobuf.SerializeToString()
        decoded = MCCSMeasurementValue()
        decoded.ParseFromString(encoded)

        self.assertEqual(decoded.mrid, OUTPUTS[FREQUENCY_MILLIHERTZ])
        self.assertEqual(decoded.WhichOneof("value"), "double_value")
        self.assertEqual(decoded.double_value, 50_010.0)
        self.assertTrue(decoded.quality.valid)
        self.assertEqual(decoded.timestamp_mccs, original.timestamp_mccs)