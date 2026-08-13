"""Conversion from validated fixture entries to Common Format Protobuf."""

from __future__ import annotations

from datetime import datetime, timezone

from google.protobuf.timestamp_pb2 import Timestamp

from pmu_gateway.config import MessageDefinition
from pmu_gateway.generated import rtd_schema_pb2


def build_measurement(
    definition: MessageDefinition,
    publish_timestamp_ms: int,
) -> rtd_schema_pb2.MCCSMeasurementValue:
    """Build one raw-Protobuf Common Format measurement for a publish cycle."""

    measurement = rtd_schema_pb2.MCCSMeasurementValue(mrid=definition.mrid)
    _set_value(measurement, definition)

    publish_timestamp = timestamp_from_epoch_ms(publish_timestamp_ms)
    measurement.timestamp_gateway.CopyFrom(publish_timestamp)
    measurement.timestamp_mccs.CopyFrom(publish_timestamp)

    if definition.field_timestamp_offset_ms is not None:
        measurement.timestamp_field.CopyFrom(
            timestamp_from_epoch_ms(
                publish_timestamp_ms - definition.field_timestamp_offset_ms
            )
        )

    for name, value in definition.quality.items():
        setattr(measurement.quality, name, value)

    return measurement


def timestamp_from_epoch_ms(timestamp_ms: int) -> Timestamp:
    """Create a Protobuf Timestamp with millisecond precision."""

    seconds, milliseconds = divmod(timestamp_ms, 1_000)
    return Timestamp(seconds=seconds, nanos=milliseconds * 1_000_000)


def _set_value(
    measurement: rtd_schema_pb2.MCCSMeasurementValue,
    definition: MessageDefinition,
) -> None:
    if definition.value_field == "timestamp_value":
        timestamp_value = Timestamp()
        timestamp_value.FromDatetime(_timestamp_datetime(definition.value))
        measurement.timestamp_value.CopyFrom(timestamp_value)
        return

    setattr(measurement, definition.value_field, definition.value)


def _timestamp_datetime(value: bool | datetime | float | int | str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError("timestamp_value definitions must contain a datetime")
    return value.astimezone(timezone.utc)