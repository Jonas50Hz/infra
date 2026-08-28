"""Deterministic per-event-second frequency aggregation into ExportRecord."""

from __future__ import annotations

from dataclasses import dataclass
import math
from uuid import UUID, uuid5

from processor_frequency_iec104_export.config import ExportMapping, Settings
from processor_frequency_iec104_export.generated import iec104_export_pb2, rtd_schema_pb2


EXPORT_NAMESPACE = UUID("f39fc794-f7e9-4d59-9c2a-5bb1d84af1b2")
_TIMESTAMP_MIN_SECONDS = -62_135_596_800
_TIMESTAMP_MAX_SECONDS = 253_402_300_799


@dataclass(frozen=True)
class ExportEnvelope:
    """One output record paired with its canonical Kafka timestamp."""

    record: iec104_export_pb2.ExportRecord
    kafka_timestamp_ms: int


@dataclass(frozen=True)
class _ExportQuality:
    """IEC 104 quality flags accumulated from valid source samples."""

    blocked: bool = False
    not_topical: bool = False
    overflow: bool = False
    substituted: bool = False

    def merged_with(self, source: rtd_schema_pb2.MCCSMeasurementValue) -> "_ExportQuality":
        """Combine source quality flags conservatively across one event-time bucket."""

        return _ExportQuality(
            blocked=self.blocked
            or (source.quality.HasField("operator_blocked") and source.quality.operator_blocked),
            not_topical=self.not_topical
            or (source.quality.HasField("old_data") and source.quality.old_data),
            overflow=self.overflow
            or (source.quality.HasField("overflow") and source.quality.overflow),
            substituted=self.substituted
            or (source.quality.HasField("substituted") and source.quality.substituted),
        )


@dataclass
class _FrequencyBucket:
    """Open per-MRID frequency samples for one event-time second."""

    second: int
    values: list[float]
    quality: _ExportQuality

    @classmethod
    def from_source(cls, second: int, source: rtd_schema_pb2.MCCSMeasurementValue) -> "_FrequencyBucket":
        """Start a bucket from one already-qualified source sample."""

        return cls(
            second=second,
            values=[float(source.double_value)],
            quality=_ExportQuality().merged_with(source),
        )

    def add(self, source: rtd_schema_pb2.MCCSMeasurementValue) -> None:
        """Add one already-qualified source sample to this bucket."""

        self.values.append(float(source.double_value))
        self.quality = self.quality.merged_with(source)

    def average(self) -> float:
        """Calculate a stable finite arithmetic mean without sum overflow."""

        scale = max(abs(value) for value in self.values)
        if scale == 0:
            return 0.0
        mean = scale * (math.fsum(value / scale for value in sorted(self.values)) / len(self.values))
        return 0.0 if mean == 0 else mean


class FrequencySecondAggregator:
    """Close one mapped MRID's event-time second on its next valid second."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._open_buckets: dict[str, _FrequencyBucket] = {}

    def process(
        self,
        source: rtd_schema_pb2.MCCSMeasurementValue,
        key: bytes | None,
    ) -> ExportEnvelope | None:
        """Accept one source sample and emit its predecessor when it closes."""

        mapping = self._settings.mapping_for(source.mrid)
        if not _is_exportable_frequency(source, key, mapping):
            return None
        second = _event_second(source)
        if second is None:
            return None

        bucket = self._open_buckets.get(source.mrid)
        if bucket is None:
            self._open_buckets[source.mrid] = _FrequencyBucket.from_source(second, source)
            return None
        if second == bucket.second:
            bucket.add(source)
            return None
        if second < bucket.second:
            return None

        self._open_buckets[source.mrid] = _FrequencyBucket.from_source(second, source)
        return build_export(source.mrid, bucket, mapping)


def build_export(
    source_mrid: str,
    bucket: _FrequencyBucket,
    mapping: ExportMapping,
) -> ExportEnvelope:
    """Create one configured M_ME_NC_1 export for a closed event-time bucket."""

    average_frequency = bucket.average()
    export_id = _export_id(source_mrid, bucket.second, average_frequency, mapping, bucket.quality)
    record = iec104_export_pb2.ExportRecord(export_id=export_id)
    record.created_at.seconds = bucket.second
    record.created_at.nanos = 0
    asdu = record.iec104_asdu
    asdu.type_id = iec104_export_pb2.IEC104_TYPE_ID_M_ME_NC_1
    asdu.common_address = mapping.common_address
    asdu.cause.code = mapping.cause_code
    information_object = asdu.information_objects.add()
    information_object.information_object_address = mapping.information_object_address
    information_object.short_float.value = float(average_frequency)
    _copy_quality(bucket.quality, information_object.short_float.quality)
    return ExportEnvelope(record=record, kafka_timestamp_ms=bucket.second * 1_000)


def _is_exportable_frequency(
    source: rtd_schema_pb2.MCCSMeasurementValue,
    key: bytes | None,
    mapping: ExportMapping | None,
) -> bool:
    if mapping is None or key != source.mrid.encode("utf-8"):
        return False
    if source.WhichOneof("value") != "double_value" or not math.isfinite(source.double_value):
        return False
    return source.HasField("quality") and source.quality.HasField("valid") and source.quality.valid


def _export_id(
    source_mrid: str,
    bucket_second: int,
    average_frequency: float,
    mapping: ExportMapping,
    quality: _ExportQuality,
) -> str:
    seed = "\0".join(
        (
            source_mrid,
            str(bucket_second),
            average_frequency.hex(),
            str(mapping.common_address),
            str(mapping.information_object_address),
            str(mapping.cause_code),
            str(int(quality.blocked)),
            str(int(quality.not_topical)),
            str(int(quality.overflow)),
            str(int(quality.substituted)),
        )
    )
    return str(uuid5(EXPORT_NAMESPACE, seed))


def _event_second(source: rtd_schema_pb2.MCCSMeasurementValue) -> int | None:
    if not source.HasField("timestamp_field"):
        return None
    timestamp = source.timestamp_field
    if not _TIMESTAMP_MIN_SECONDS <= timestamp.seconds <= _TIMESTAMP_MAX_SECONDS:
        return None
    if not 0 <= timestamp.nanos <= 999_999_999:
        return None
    return timestamp.seconds


def _copy_quality(
    source: _ExportQuality,
    target: iec104_export_pb2.Iec104Quality,
) -> None:
    target.substituted = source.substituted
    target.blocked = source.blocked
    target.overflow = source.overflow
    target.not_topical = source.not_topical