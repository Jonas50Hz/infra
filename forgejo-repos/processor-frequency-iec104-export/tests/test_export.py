"""Deterministic raw-Protobuf export construction tests."""

from __future__ import annotations

import math
import unittest

from processor_frequency_iec104_export.config import Settings
from processor_frequency_iec104_export.export import build_export
from processor_frequency_iec104_export.generated import iec104_export_pb2, rtd_schema_pb2


class ExportTests(unittest.TestCase):
    """Require mapped valid frequency values to yield one configured M_ME_NC_1."""

    def test_builds_deterministic_timestamp_and_quality_aligned_export(self) -> None:
        source = _frequency(1, 50.01)
        source.quality.substituted = True
        source.quality.operator_blocked = True
        source.quality.overflow = True
        source.quality.old_data = True
        timestamp_ms = 1_726_000_123_456

        first = build_export(source, source.mrid.encode("utf-8"), timestamp_ms, _settings())
        second = build_export(source, source.mrid.encode("utf-8"), timestamp_ms, _settings())

        self.assertIsNotNone(first)
        self.assertEqual(first, second)
        assert first is not None
        record = first.record
        self.assertEqual(record.created_at.ToMilliseconds(), timestamp_ms)
        self.assertEqual(record.iec104_asdu.type_id, iec104_export_pb2.IEC104_TYPE_ID_M_ME_NC_1)
        self.assertEqual(record.iec104_asdu.common_address, 1001)
        self.assertEqual(record.iec104_asdu.cause.code, 3)
        information_object = record.iec104_asdu.information_objects[0]
        self.assertEqual(information_object.information_object_address, 1001)
        self.assertAlmostEqual(information_object.short_float.value, 50.01, places=4)
        self.assertTrue(information_object.short_float.quality.substituted)
        self.assertTrue(information_object.short_float.quality.blocked)
        self.assertTrue(information_object.short_float.quality.overflow)
        self.assertTrue(information_object.short_float.quality.not_topical)

    def test_maps_every_configured_gateway_frequency_to_its_iec_point(self) -> None:
        settings = _settings()
        for bay in range(1, 6):
            source = _frequency(bay, 50.0 + bay / 100)
            envelope = build_export(source, source.mrid.encode("utf-8"), 1_726_000_123_456, settings)

            self.assertIsNotNone(envelope)
            assert envelope is not None
            record = envelope.record
            self.assertEqual(record.iec104_asdu.common_address, 1000 + bay)
            self.assertEqual(
                record.iec104_asdu.information_objects[0].information_object_address,
                1001,
            )
            self.assertEqual(record.iec104_asdu.cause.code, 3)

    def test_rejects_wrong_key_invalid_and_nonfinite_sources(self) -> None:
        source = _frequency(1, 50.01)
        settings = _settings()

        self.assertIsNone(build_export(source, b"wrong", 1_726_000_123_456, settings))
        source.quality.valid = False
        self.assertIsNone(build_export(source, source.mrid.encode("utf-8"), 1_726_000_123_456, settings))
        source.quality.valid = True
        source.double_value = math.nan
        self.assertIsNone(build_export(source, source.mrid.encode("utf-8"), 1_726_000_123_456, settings))
        source.double_value = 50.01
        self.assertIsNone(build_export(source, source.mrid.encode("utf-8"), 0, settings))
        unconfigured = _frequency(99, 50.01)
        self.assertIsNone(
            build_export(
                unconfigured,
                unconfigured.mrid.encode("utf-8"),
                1_726_000_123_456,
                settings,
            )
        )

    def test_round_trips_raw_export_protobuf(self) -> None:
        source = _frequency(1, 50.01)
        envelope = build_export(source, source.mrid.encode("utf-8"), 1_726_000_123_456, _settings())

        self.assertIsNotNone(envelope)
        assert envelope is not None
        decoded = iec104_export_pb2.ExportRecord()
        decoded.ParseFromString(envelope.record.SerializeToString())

        self.assertEqual(decoded.export_id, envelope.record.export_id)
        self.assertEqual(decoded.iec104_asdu.information_objects[0].short_float.value, envelope.record.iec104_asdu.information_objects[0].short_float.value)


def _frequency(bay: int, value: float) -> rtd_schema_pb2.MCCSMeasurementValue:
    source = rtd_schema_pb2.MCCSMeasurementValue(
        mrid=f"urn:wama:poc:pmu:bay-{bay:02}:frequency",
        double_value=value,
    )
    source.quality.valid = True
    return source


def _settings() -> Settings:
    return Settings.from_environment({})


if __name__ == "__main__":
    unittest.main()