"""Tests for raw-Protobuf derived frequency serialization."""

from __future__ import annotations

import unittest

from processor_frequency_scale.generated.rtd_schema_pb2 import MCCSMeasurementValue
from processor_frequency_scale.pipeline import OUTPUT_MRID, SOURCE_KEY, SOURCE_MRID, transform


class ProtobufSerializationTests(unittest.TestCase):
    """Keep the derived value on the raw Common Format contract."""

    def test_round_trips_a_derived_common_format_measurement(self) -> None:
        original = MCCSMeasurementValue(
            mrid=SOURCE_MRID,
            double_value=50.01,
        )
        original.quality.valid = True
        original.timestamp_mccs.seconds = 1_726_000_123
        derived = transform(original, SOURCE_KEY, 1_726_000_123_000)

        self.assertIsNotNone(derived)
        assert derived is not None
        encoded = derived.measurement.SerializeToString()
        decoded = MCCSMeasurementValue()
        decoded.ParseFromString(encoded)

        self.assertEqual(decoded.mrid, OUTPUT_MRID)
        self.assertEqual(decoded.WhichOneof("value"), "double_value")
        self.assertEqual(decoded.double_value, 50_010.0)
        self.assertTrue(decoded.quality.valid)
        self.assertEqual(decoded.timestamp_mccs, original.timestamp_mccs)