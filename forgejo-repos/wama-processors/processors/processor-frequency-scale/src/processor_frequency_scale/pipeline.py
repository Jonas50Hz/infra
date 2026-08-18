"""Stateless Common Format transformation for the fake PMU frequency."""

from __future__ import annotations

from dataclasses import dataclass

from processor_frequency_scale.generated.rtd_schema_pb2 import MCCSMeasurementValue

SOURCE_MRID = "urn:wama:poc:pmu:bay-01:frequency"
OUTPUT_MRID = "urn:wama:poc:pmu:bay-01:frequency-millihertz"
SOURCE_KEY = SOURCE_MRID.encode("utf-8")
OUTPUT_KEY = OUTPUT_MRID.encode("utf-8")
HERTZ_TO_MILLIHERTZ = 1_000.0


@dataclass(frozen=True)
class DerivedMeasurement:
    """A transformed measurement with the Kafka timestamp to publish."""

    measurement: MCCSMeasurementValue
    kafka_timestamp_ms: int


def transform(
    measurement: MCCSMeasurementValue,
    key: bytes | None,
    kafka_timestamp_ms: int,
) -> DerivedMeasurement | None:
    """Scale only the exact PMU frequency source without mutating its input."""

    if key != SOURCE_KEY or measurement.mrid != SOURCE_MRID:
        return None
    if measurement.WhichOneof("value") != "double_value":
        return None

    derived = MCCSMeasurementValue()
    derived.CopyFrom(measurement)
    derived.mrid = OUTPUT_MRID
    derived.double_value = measurement.double_value * HERTZ_TO_MILLIHERTZ
    return DerivedMeasurement(
        measurement=derived,
        kafka_timestamp_ms=kafka_timestamp_ms,
    )


def output_key(measurement: MCCSMeasurementValue) -> bytes:
    """Return the deterministic Kafka key for a derived measurement."""

    return measurement.mrid.encode("utf-8")


def preserve_kafka_timestamp(
    derived: DerivedMeasurement,
    _key: bytes | None,
    _timestamp: int,
    _headers: object,
) -> int:
    """Use the source Kafka timestamp when Quixstreams publishes the output."""

    return derived.kafka_timestamp_ms