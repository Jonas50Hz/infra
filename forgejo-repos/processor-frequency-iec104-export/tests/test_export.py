"""Per-event-second raw-Protobuf frequency export tests."""

from __future__ import annotations

import math
import unittest

from processor_frequency_iec104_export.config import Settings
from processor_frequency_iec104_export.export import FrequencySecondAggregator
from processor_frequency_iec104_export.generated import iec104_export_pb2, rtd_schema_pb2


class ExportTests(unittest.TestCase):
    """Require mapped valid frequencies to close into one second-level export."""

    def test_averages_timestamp_field_boundary_membership_at_bucket_start(self) -> None:
        aggregator = FrequencySecondAggregator(_settings())
        bucket_second = 1_726_000_123

        self.assertIsNone(_process(aggregator, _frequency(1, 49.0, bucket_second, 0)))
        self.assertIsNone(_process(aggregator, _frequency(1, 50.0, bucket_second, 500_000_000)))
        self.assertIsNone(_process(aggregator, _frequency(1, 51.0, bucket_second, 999_999_999)))

        envelope = _process(aggregator, _frequency(1, 80.0, bucket_second + 1, 0))

        self.assertIsNotNone(envelope)
        assert envelope is not None
        record = envelope.record
        self.assertEqual(envelope.kafka_timestamp_ms, bucket_second * 1_000)
        self.assertEqual(record.created_at.seconds, bucket_second)
        self.assertEqual(record.created_at.nanos, 0)
        self.assertEqual(record.iec104_asdu.type_id, iec104_export_pb2.IEC104_TYPE_ID_M_ME_NC_1)
        self.assertEqual(record.iec104_asdu.common_address, 1001)
        self.assertEqual(record.iec104_asdu.cause.code, 3)
        information_object = record.iec104_asdu.information_objects[0]
        self.assertEqual(information_object.information_object_address, 1001)
        self.assertAlmostEqual(information_object.short_float.value, 50.0, places=4)

    def test_waits_to_close_next_bucket_and_emits_each_bucket_once(self) -> None:
        aggregator = FrequencySecondAggregator(_settings())
        bucket_second = 1_726_000_123

        self.assertIsNone(_process(aggregator, _frequency(1, 50.0, bucket_second, 0)))
        first = _process(aggregator, _frequency(1, 51.0, bucket_second + 1, 0))
        self.assertIsNotNone(first)
        self.assertIsNone(_process(aggregator, _frequency(1, 53.0, bucket_second + 1, 999_999_999)))

        second = _process(aggregator, _frequency(1, 55.0, bucket_second + 2, 0))

        self.assertIsNotNone(second)
        assert first is not None
        assert second is not None
        self.assertEqual(first.record.created_at.seconds, bucket_second)
        self.assertAlmostEqual(
            second.record.iec104_asdu.information_objects[0].short_float.value,
            52.0,
            places=4,
        )

    def test_keeps_mrid_mappings_independent(self) -> None:
        aggregator = FrequencySecondAggregator(_settings())
        bucket_second = 1_726_000_123

        self.assertIsNone(_process(aggregator, _frequency(1, 50.0, bucket_second, 0)))
        self.assertIsNone(_process(aggregator, _frequency(2, 60.0, bucket_second, 0)))

        first = _process(aggregator, _frequency(1, 51.0, bucket_second + 1, 0))
        second = _process(aggregator, _frequency(2, 61.0, bucket_second + 1, 0))

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        assert first is not None
        assert second is not None
        self.assertEqual(first.record.iec104_asdu.common_address, 1001)
        self.assertEqual(second.record.iec104_asdu.common_address, 1002)

    def test_ignores_late_samples_from_an_already_closed_bucket(self) -> None:
        aggregator = FrequencySecondAggregator(_settings())
        bucket_second = 1_726_000_123

        self.assertIsNone(_process(aggregator, _frequency(1, 50.0, bucket_second, 0)))
        first = _process(aggregator, _frequency(1, 52.0, bucket_second + 1, 0))
        self.assertIsNotNone(first)
        self.assertIsNone(_process(aggregator, _frequency(1, 100.0, bucket_second, 999_999_999)))

        second = _process(aggregator, _frequency(1, 54.0, bucket_second + 2, 0))

        self.assertIsNotNone(second)
        assert first is not None
        assert second is not None
        self.assertAlmostEqual(
            first.record.iec104_asdu.information_objects[0].short_float.value,
            50.0,
            places=4,
        )
        self.assertAlmostEqual(
            second.record.iec104_asdu.information_objects[0].short_float.value,
            52.0,
            places=4,
        )

    def test_rejects_wrong_key_invalid_and_nonfinite_sources(self) -> None:
        source = _frequency(1, 50.01)
        aggregator = FrequencySecondAggregator(_settings())

        self.assertIsNone(aggregator.process(source, b"wrong"))
        source.quality.valid = False
        self.assertIsNone(_process(aggregator, source))
        source.quality.valid = True
        source.double_value = math.nan
        self.assertIsNone(_process(aggregator, source))
        unconfigured = _frequency(99, 50.01)
        self.assertIsNone(_process(aggregator, unconfigured))

    def test_ignores_missing_or_invalid_timestamp_field(self) -> None:
        aggregator = FrequencySecondAggregator(_settings())
        bucket_second = 1_726_000_123

        self.assertIsNone(_process(aggregator, _frequency(1, 99.0, None, None)))
        self.assertIsNone(_process(aggregator, _frequency(1, 99.0, bucket_second, 1_000_000_000)))
        self.assertIsNone(_process(aggregator, _frequency(1, 50.0, bucket_second, 0)))

        envelope = _process(aggregator, _frequency(1, 51.0, bucket_second + 1, 0))

        self.assertIsNotNone(envelope)
        assert envelope is not None
        decoded = iec104_export_pb2.ExportRecord()
        decoded.ParseFromString(envelope.record.SerializeToString())

        self.assertEqual(decoded.export_id, envelope.record.export_id)
        self.assertEqual(
            decoded.iec104_asdu.information_objects[0].short_float.value,
            envelope.record.iec104_asdu.information_objects[0].short_float.value,
        )

    def test_builds_deterministic_identifier_and_aggregated_quality(self) -> None:
        bucket_second = 1_726_000_123
        first = self._closed_export([50.0, 51.0], bucket_second)
        second = self._closed_export([51.0, 50.0], bucket_second)

        self.assertEqual(first.record.export_id, second.record.export_id)
        quality = first.record.iec104_asdu.information_objects[0].short_float.quality
        self.assertTrue(quality.substituted)
        self.assertTrue(quality.blocked)
        self.assertTrue(quality.overflow)
        self.assertTrue(quality.not_topical)

    def _closed_export(self, values: list[float], bucket_second: int):
        aggregator = FrequencySecondAggregator(_settings())
        for value in values:
            source = _frequency(1, value, bucket_second, 0)
            source.quality.substituted = True
            source.quality.operator_blocked = True
            source.quality.overflow = True
            source.quality.old_data = True
            self.assertIsNone(_process(aggregator, source))
        envelope = _process(aggregator, _frequency(1, 60.0, bucket_second + 1, 0))
        assert envelope is not None
        return envelope


def _process(
    aggregator: FrequencySecondAggregator,
    source: rtd_schema_pb2.MCCSMeasurementValue,
):
    return aggregator.process(source, source.mrid.encode("utf-8"))


def _frequency(
    bay: int,
    value: float,
    seconds: int | None = 1_726_000_123,
    nanos: int | None = 0,
) -> rtd_schema_pb2.MCCSMeasurementValue:
    source = rtd_schema_pb2.MCCSMeasurementValue(
        mrid=f"urn:wama:poc:pmu:bay-{bay:02}:frequency",
        double_value=value,
    )
    source.quality.valid = True
    if seconds is not None and nanos is not None:
        source.timestamp_field.seconds = seconds
        source.timestamp_field.nanos = nanos
    return source


def _settings() -> Settings:
    return Settings.from_environment({})


if __name__ == "__main__":
    unittest.main()