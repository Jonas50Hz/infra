"""Raw Common Format Protobuf conversion for the LFR processor boundary."""

from __future__ import annotations

from processor_lfr_frequency_provision.engine import IncomingMeasurement, QualityEvidence
from processor_lfr_frequency_provision.state import PendingPublication
from processor_lfr_frequency_provision.generated import rtd_schema_pb2


def incoming_measurement(
    source: rtd_schema_pb2.MCCSMeasurementValue,
    input_id: str,
) -> IncomingMeasurement:
    """Convert one decoded raw-Protobuf source record into domain input."""

    value = (
        source.double_value
        if source.WhichOneof("value") == "double_value"
        else None
    )
    quality = source.quality if source.HasField("quality") else None
    return IncomingMeasurement(
        input_id=input_id,
        mrid=source.mrid,
        double_value=value,
        quality=QualityEvidence(
            valid=(quality.valid if quality is not None and quality.HasField("valid") else None),
            substituted=(
                quality.substituted
                if quality is not None and quality.HasField("substituted")
                else False
            ),
            operator_blocked=(
                quality.operator_blocked
                if quality is not None and quality.HasField("operator_blocked")
                else False
            ),
            overflow=(
                quality.overflow
                if quality is not None and quality.HasField("overflow")
                else False
            ),
            old_data=(
                quality.old_data
                if quality is not None and quality.HasField("old_data")
                else False
            ),
        ),
        timestamp_field_ms=(
            source.timestamp_field.ToMilliseconds()
            if source.HasField("timestamp_field")
            else None
        ),
    )


def preferred_measurement(
    publication: PendingPublication,
) -> rtd_schema_pb2.MCCSMeasurementValue:
    """Create the raw Common Format preferred-frequency value for an outbox item."""

    measurement = rtd_schema_pb2.MCCSMeasurementValue(
        mrid=publication.output_mrid,
        double_value=publication.frequency_hz,
    )
    measurement.timestamp_field.seconds = publication.second + 1
    measurement.timestamp_mccs.seconds = publication.closed_at_ms // 1_000
    measurement.timestamp_mccs.nanos = (publication.closed_at_ms % 1_000) * 1_000_000
    measurement.quality.valid = True
    return measurement