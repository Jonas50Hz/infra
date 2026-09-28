"""Tests for raw Common Format input and preferred-frequency output encoding."""

from __future__ import annotations

import unittest

from processor_lfr_frequency_provision.codec import incoming_measurement, preferred_measurement
from processor_lfr_frequency_provision.generated import rtd_schema_pb2
from processor_lfr_frequency_provision.selection import PmuQuality
from processor_lfr_frequency_provision.state import PendingPublication


class CommonFormatCodecTests(unittest.TestCase):
    """Keep LFR input evidence and derived output on raw Protobuf."""

    def test_decodes_generic_quality_evidence_and_field_timestamp(self) -> None:
        source = rtd_schema_pb2.MCCSMeasurementValue(
            mrid="urn:wama:test:pmu-a:frequency",
            double_value=50.01,
        )
        source.timestamp_field.seconds = 1_726_000_123
        source.timestamp_field.nanos = 456_000_000
        source.quality.valid = True
        source.quality.overflow = True

        decoded = incoming_measurement(source, "LiveMeasurement:0:12")

        self.assertEqual(decoded.input_id, "LiveMeasurement:0:12")
        self.assertEqual(decoded.double_value, 50.01)
        self.assertEqual(decoded.timestamp_field_ms, 1_726_000_123_456)
        self.assertTrue(decoded.quality.valid)
        self.assertTrue(decoded.quality.overflow)

    def test_emits_a_preferred_frequency_with_final_second_and_close_timestamp(self) -> None:
        publication = PendingPublication(
            publication_id="urn:wama:test:lfr:preferred-frequency:1726000123",
            closed_at_ms=1_726_000_124_600,
            frequency_hz=50.0125,
            output_mrid="urn:wama:test:lfr:preferred-frequency",
            quality=PmuQuality.VERY_GOOD,
            second=1_726_000_123,
        )

        encoded = preferred_measurement(publication)
        raw = encoded.SerializeToString()
        decoded = rtd_schema_pb2.MCCSMeasurementValue()
        decoded.ParseFromString(raw)

        self.assertEqual(decoded.mrid, publication.output_mrid)
        self.assertEqual(decoded.WhichOneof("value"), "double_value")
        self.assertAlmostEqual(decoded.double_value, 50.0125)
        self.assertEqual(decoded.timestamp_field.seconds, publication.second + 1)
        self.assertEqual(decoded.timestamp_mccs.ToMilliseconds(), publication.closed_at_ms)
        self.assertTrue(decoded.quality.valid)


if __name__ == "__main__":
    unittest.main()