"""Tests for raw-Protobuf Common Format processing fixtures."""

from __future__ import annotations

import unittest

from processor_template.generated.rtd_schema_pb2 import MCCSMeasurementValue


class ProtobufSerializationTests(unittest.TestCase):
    """Keep a fixture pattern processor authors can adapt to expected output."""

    def test_round_trips_a_common_format_measurement(self) -> None:
        original = MCCSMeasurementValue(
            mrid="urn:wama:poc:test:frequency",
            double_value=50.01,
        )
        original.quality.valid = True

        encoded = original.SerializeToString()
        decoded = MCCSMeasurementValue()
        decoded.ParseFromString(encoded)

        self.assertEqual(decoded.mrid, original.mrid)
        self.assertEqual(decoded.WhichOneof("value"), "double_value")
        self.assertEqual(decoded.double_value, original.double_value)
        self.assertTrue(decoded.quality.valid)