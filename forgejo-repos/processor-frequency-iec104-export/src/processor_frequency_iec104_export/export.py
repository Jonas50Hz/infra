"""Deterministic conversion from one valid Common Format frequency to ExportRecord."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
import math
from uuid import UUID, uuid5

from processor_frequency_iec104_export.config import Settings
from processor_frequency_iec104_export.generated import iec104_export_pb2, rtd_schema_pb2


EXPORT_NAMESPACE = UUID("f39fc794-f7e9-4d59-9c2a-5bb1d84af1b2")


@dataclass(frozen=True)
class ExportEnvelope:
    """One output record paired with its inherited Kafka timestamp."""

    record: iec104_export_pb2.ExportRecord
    kafka_timestamp_ms: int


def build_export(
    source: rtd_schema_pb2.MCCSMeasurementValue,
    key: bytes | None,
    kafka_timestamp_ms: int,
    settings: Settings,
) -> ExportEnvelope | None:
    """Create a configured M_ME_NC_1 export only from an explicitly valid frequency."""

    if not _is_exportable_frequency(source, key, kafka_timestamp_ms, settings):
        return None
    payload = source.SerializeToString(deterministic=True)
    export_id = _export_id(payload, kafka_timestamp_ms, settings)
    record = iec104_export_pb2.ExportRecord(export_id=export_id)
    record.created_at.seconds = kafka_timestamp_ms // 1_000
    record.created_at.nanos = (kafka_timestamp_ms % 1_000) * 1_000_000
    asdu = record.iec104_asdu
    asdu.type_id = iec104_export_pb2.IEC104_TYPE_ID_M_ME_NC_1
    asdu.common_address = settings.common_address
    asdu.cause.code = settings.cause_code
    information_object = asdu.information_objects.add()
    information_object.information_object_address = settings.information_object_address
    information_object.short_float.value = float(source.double_value)
    _copy_quality(source, information_object.short_float.quality)
    return ExportEnvelope(record=record, kafka_timestamp_ms=kafka_timestamp_ms)


def _is_exportable_frequency(
    source: rtd_schema_pb2.MCCSMeasurementValue,
    key: bytes | None,
    kafka_timestamp_ms: int,
    settings: Settings,
) -> bool:
    if kafka_timestamp_ms <= 0:
        return False
    if source.mrid != settings.source_mrid or key != settings.source_mrid.encode("utf-8"):
        return False
    if source.WhichOneof("value") != "double_value" or not math.isfinite(source.double_value):
        return False
    return source.HasField("quality") and source.quality.HasField("valid") and source.quality.valid


def _export_id(payload: bytes, kafka_timestamp_ms: int, settings: Settings) -> str:
    seed = b"\0".join(
        (
            settings.source_mrid.encode("utf-8"),
            str(kafka_timestamp_ms).encode("ascii"),
            str(settings.common_address).encode("ascii"),
            str(settings.information_object_address).encode("ascii"),
            str(settings.cause_code).encode("ascii"),
            sha256(payload).hexdigest().encode("ascii"),
        )
    )
    return str(uuid5(EXPORT_NAMESPACE, seed.decode("ascii")))


def _copy_quality(
    source: rtd_schema_pb2.MCCSMeasurementValue,
    target: iec104_export_pb2.Iec104Quality,
) -> None:
    if not source.HasField("quality"):
        return
    target.substituted = source.quality.HasField("substituted") and source.quality.substituted
    target.blocked = source.quality.HasField("operator_blocked") and source.quality.operator_blocked
    target.overflow = source.quality.HasField("overflow") and source.quality.overflow
    target.not_topical = source.quality.HasField("old_data") and source.quality.old_data