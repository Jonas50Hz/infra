"""Validation for typed, monitor-direction IEC 104 export records."""

from __future__ import annotations

from datetime import timezone
import math
from typing import Any
from uuid import UUID

from google.protobuf.message import DecodeError

from iec104_common.generated import iec104_export_pb2

MAX_ASDU_BYTES = 249
MAX_COMMON_ADDRESS = 0xFFFF
MAX_INFORMATION_OBJECT_ADDRESS = 0xFFFFFF
MAX_INFORMATION_OBJECTS = 127
MAX_SERIALIZED_RECORD_BYTES = 64 * 1024

_MONITOR_CAUSE_CODES = frozenset({1, 2, 3, 4, 5, *range(20, 37)})

_VALUE_FIELDS_BY_TYPE = {
    iec104_export_pb2.IEC104_TYPE_ID_M_SP_NA_1: "single_point",
    iec104_export_pb2.IEC104_TYPE_ID_M_DP_NA_1: "double_point",
    iec104_export_pb2.IEC104_TYPE_ID_M_ME_NC_1: "short_float",
}


class ContractValidationError(ValueError):
    """Raised when an Export record cannot be encoded as a supported IEC ASDU."""


def parse_kafka_record(record: Any) -> iec104_export_pb2.ExportRecord:
    """Decode and validate one keyed Kafka record before it reaches IEC 104."""

    message = iec104_export_pb2.ExportRecord()
    try:
        message.ParseFromString(record.value)
    except DecodeError as error:
        raise ContractValidationError("Export payload is not valid raw Protobuf") from error
    validate_export_record(message)
    if record.key != message.export_id.encode("utf-8"):
        raise ContractValidationError("Export Kafka key does not match export_id")
    if record.timestamp != _timestamp_milliseconds(message.created_at):
        raise ContractValidationError("Export Kafka timestamp does not match created_at")
    return message


def validate_export_record(record: iec104_export_pb2.ExportRecord) -> None:
    """Ensure one Export record can become one supported monitor-direction ASDU."""

    _canonical_uuid(record.export_id, "export_id")
    if not record.HasField("created_at"):
        raise ContractValidationError("created_at is required")
    _timestamp_milliseconds(record.created_at)

    if record.WhichOneof("target") != "iec104_asdu":
        raise ContractValidationError("Export target must be iec104_asdu")
    if record.ByteSize() > MAX_SERIALIZED_RECORD_BYTES:
        raise ContractValidationError(
            f"serialized Export record must not exceed {MAX_SERIALIZED_RECORD_BYTES} bytes"
        )
    _validate_asdu(record.iec104_asdu)


def _validate_asdu(asdu: iec104_export_pb2.Iec104Asdu) -> None:
    expected_value_field = _VALUE_FIELDS_BY_TYPE.get(asdu.type_id)
    if expected_value_field is None:
        raise ContractValidationError("IEC 104 type_id is not supported")
    if not 1 <= asdu.common_address <= MAX_COMMON_ADDRESS:
        raise ContractValidationError(
            f"common_address must be between 1 and {MAX_COMMON_ADDRESS}"
        )
    if not asdu.HasField("cause"):
        raise ContractValidationError("cause is required")
    _validate_cause(asdu.cause)

    object_count = len(asdu.information_objects)
    if not 1 <= object_count <= MAX_INFORMATION_OBJECTS:
        raise ContractValidationError(
            f"information_objects must contain between 1 and {MAX_INFORMATION_OBJECTS} items"
        )

    seen_addresses: set[int] = set()
    for information_object in asdu.information_objects:
        address = information_object.information_object_address
        if not 1 <= address <= MAX_INFORMATION_OBJECT_ADDRESS:
            raise ContractValidationError(
                "information_object_address must be between 1 and "
                f"{MAX_INFORMATION_OBJECT_ADDRESS}"
            )
        if address in seen_addresses:
            raise ContractValidationError("information_object_address values must be unique")
        seen_addresses.add(address)

        value_field = information_object.WhichOneof("value")
        if value_field != expected_value_field:
            raise ContractValidationError(
                f"{_type_name(asdu.type_id)} requires {expected_value_field} information objects"
            )
        _validate_value(information_object, value_field)

    if _encoded_asdu_bytes(asdu, expected_value_field) > MAX_ASDU_BYTES:
        raise ContractValidationError(
            f"ASDU exceeds the IEC 104 maximum of {MAX_ASDU_BYTES} bytes"
        )


def _validate_cause(cause: iec104_export_pb2.Iec104CauseOfTransmission) -> None:
    if cause.code not in _MONITOR_CAUSE_CODES:
        raise ContractValidationError("cause.code is not supported for monitor-direction ASDUs")


def _validate_value(
    information_object: iec104_export_pb2.Iec104InformationObject,
    value_field: str,
) -> None:
    if value_field == "single_point":
        _validate_quality(information_object.single_point.quality, False)
        return
    if value_field == "double_point":
        if information_object.double_point.value not in {
            iec104_export_pb2.IEC104_DOUBLE_POINT_INTERMEDIATE,
            iec104_export_pb2.IEC104_DOUBLE_POINT_OFF,
            iec104_export_pb2.IEC104_DOUBLE_POINT_ON,
            iec104_export_pb2.IEC104_DOUBLE_POINT_INDETERMINATE,
        }:
            raise ContractValidationError("double_point.value is not a valid IEC double-point state")
        _validate_quality(information_object.double_point.quality, False)
        return
    if value_field == "short_float":
        if not math.isfinite(information_object.short_float.value):
            raise ContractValidationError("short_float.value must be finite")
        _validate_quality(information_object.short_float.quality, True)
        return
    raise ContractValidationError("information object has no supported value")


def _validate_quality(quality: iec104_export_pb2.Iec104Quality, supports_overflow: bool) -> None:
    if quality.overflow and not supports_overflow:
        raise ContractValidationError("quality.overflow is valid only for M_ME_NC_1")


def _encoded_asdu_bytes(asdu: iec104_export_pb2.Iec104Asdu, value_field: str) -> int:
    value_bytes = 5 if value_field == "short_float" else 1
    information_object_count = len(asdu.information_objects)
    address_bytes = 3 if _uses_sequence_addressing(asdu) else 3 * information_object_count
    return 6 + address_bytes + value_bytes * information_object_count


def _uses_sequence_addressing(asdu: iec104_export_pb2.Iec104Asdu) -> bool:
    first_address = asdu.information_objects[0].information_object_address
    return all(
        information_object.information_object_address == first_address + index
        for index, information_object in enumerate(asdu.information_objects)
    )


def _timestamp_milliseconds(timestamp: Any) -> int:
    try:
        timestamp.ToDatetime(tzinfo=timezone.utc)
    except ValueError as error:
        raise ContractValidationError("created_at is outside the Protobuf timestamp range") from error
    milliseconds = timestamp.seconds * 1_000 + timestamp.nanos // 1_000_000
    if milliseconds <= 0:
        raise ContractValidationError("created_at must be after the Unix epoch")
    return milliseconds


def _canonical_uuid(value: str, field_name: str) -> None:
    try:
        parsed = UUID(value)
    except (TypeError, ValueError) as error:
        raise ContractValidationError(f"{field_name} must be a canonical UUID") from error
    if str(parsed) != value:
        raise ContractValidationError(f"{field_name} must be a lowercase canonical UUID")


def _type_name(type_id: int) -> str:
    return iec104_export_pb2.Iec104TypeId.Name(type_id)