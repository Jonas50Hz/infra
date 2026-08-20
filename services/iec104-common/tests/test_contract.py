"""Focused validation tests for raw-Protobuf IEC 104 export records."""

from __future__ import annotations

from datetime import datetime, timezone
import math
from types import SimpleNamespace
import unittest

from google.protobuf.timestamp_pb2 import Timestamp

from iec104_common.contract import ContractValidationError, parse_kafka_record, validate_export_record
from iec104_common.generated import iec104_export_pb2


class ExportContractTests(unittest.TestCase):
    """Reject contracts that cannot become a supported outbound IEC ASDU."""

    def test_accepts_sequential_single_points_and_matching_kafka_metadata(self) -> None:
        record = _record(iec104_export_pb2.IEC104_TYPE_ID_M_SP_NA_1)
        _add_single_point(record, 1, True)
        _add_single_point(record, 2, False)

        validate_export_record(record)
        parsed = parse_kafka_record(
            SimpleNamespace(
                key=record.export_id.encode("utf-8"),
                value=record.SerializeToString(),
                timestamp=_timestamp_milliseconds(record.created_at),
            )
        )

        self.assertEqual(parsed, record)

    def test_rejects_value_type_that_does_not_match_the_asdu_type(self) -> None:
        record = _record(iec104_export_pb2.IEC104_TYPE_ID_M_SP_NA_1)
        information_object = record.iec104_asdu.information_objects.add()
        information_object.information_object_address = 1
        information_object.short_float.value = 50.0

        with self.assertRaisesRegex(ContractValidationError, "requires single_point"):
            validate_export_record(record)

    def test_rejects_non_finite_short_float(self) -> None:
        record = _record(iec104_export_pb2.IEC104_TYPE_ID_M_ME_NC_1)
        information_object = record.iec104_asdu.information_objects.add()
        information_object.information_object_address = 1
        information_object.short_float.value = math.inf

        with self.assertRaisesRegex(ContractValidationError, "must be finite"):
            validate_export_record(record)

    def test_rejects_overflow_on_single_point(self) -> None:
        record = _record(iec104_export_pb2.IEC104_TYPE_ID_M_SP_NA_1)
        _add_single_point(record, 1, True)
        record.iec104_asdu.information_objects[0].single_point.quality.overflow = True

        with self.assertRaisesRegex(ContractValidationError, "only for M_ME_NC_1"):
            validate_export_record(record)

    def test_accepts_all_iec_double_point_states(self) -> None:
        for state in (
            iec104_export_pb2.IEC104_DOUBLE_POINT_INTERMEDIATE,
            iec104_export_pb2.IEC104_DOUBLE_POINT_OFF,
            iec104_export_pb2.IEC104_DOUBLE_POINT_ON,
            iec104_export_pb2.IEC104_DOUBLE_POINT_INDETERMINATE,
        ):
            with self.subTest(state=state):
                record = _record(iec104_export_pb2.IEC104_TYPE_ID_M_DP_NA_1)
                information_object = record.iec104_asdu.information_objects.add()
                information_object.information_object_address = 1
                information_object.double_point.value = state

                validate_export_record(record)

    def test_rejects_asdu_larger_than_iec_104_allows(self) -> None:
        record = _record(iec104_export_pb2.IEC104_TYPE_ID_M_SP_NA_1)
        for address in range(1, 123, 2):
            _add_single_point(record, address, True)

        with self.assertRaisesRegex(ContractValidationError, "maximum of 249"):
            validate_export_record(record)

    def test_rejects_control_direction_cause(self) -> None:
        record = _record(iec104_export_pb2.IEC104_TYPE_ID_M_SP_NA_1)
        _add_single_point(record, 1, True)
        record.iec104_asdu.cause.code = 6

        with self.assertRaisesRegex(ContractValidationError, "monitor-direction"):
            validate_export_record(record)

    def test_rejects_mismatched_kafka_key_and_timestamp(self) -> None:
        record = _record(iec104_export_pb2.IEC104_TYPE_ID_M_ME_NC_1)
        information_object = record.iec104_asdu.information_objects.add()
        information_object.information_object_address = 1
        information_object.short_float.value = 50.01

        with self.assertRaisesRegex(ContractValidationError, "Kafka key"):
            parse_kafka_record(
                SimpleNamespace(
                    key=b"different",
                    value=record.SerializeToString(),
                    timestamp=_timestamp_milliseconds(record.created_at),
                )
            )
        with self.assertRaisesRegex(ContractValidationError, "Kafka timestamp"):
            parse_kafka_record(
                SimpleNamespace(
                    key=record.export_id.encode("utf-8"),
                    value=record.SerializeToString(),
                    timestamp=_timestamp_milliseconds(record.created_at) + 1,
                )
            )


def _record(type_id: int) -> iec104_export_pb2.ExportRecord:
    record = iec104_export_pb2.ExportRecord(
        export_id="4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237",
    )
    timestamp = Timestamp()
    timestamp.FromDatetime(datetime(2026, 8, 19, 12, 0, tzinfo=timezone.utc))
    record.created_at.CopyFrom(timestamp)
    record.iec104_asdu.type_id = type_id
    record.iec104_asdu.common_address = 1
    record.iec104_asdu.cause.code = 3
    return record


def _add_single_point(
    record: iec104_export_pb2.ExportRecord,
    address: int,
    value: bool,
) -> None:
    information_object = record.iec104_asdu.information_objects.add()
    information_object.information_object_address = address
    information_object.single_point.value = value


def _timestamp_milliseconds(timestamp: Timestamp) -> int:
    return timestamp.seconds * 1_000 + timestamp.nanos // 1_000_000


if __name__ == "__main__":
    unittest.main()