"""Small fixtures for processor authors' domain-focused unit tests."""

from __future__ import annotations

from wama_processor.definition import InputMeasurement, ProcessorDefinition
from wama_processor.generated.rtd_schema_pb2 import MCCSMeasurementValue


def input_measurement(
    definition: ProcessorDefinition,
    name: str,
    double_value: float | None,
    *,
    is_valid: bool = True,
    kafka_timestamp_ms: int = 0,
) -> InputMeasurement:
    """Create a named input fixture without Kafka or Protobuf test plumbing."""

    mrid = definition.inputs.get(name)
    if mrid is None:
        raise ValueError(f"Unknown processor input {name!r}")

    measurement = MCCSMeasurementValue(mrid=mrid)
    if double_value is None:
        measurement.int_value = 0
    else:
        measurement.double_value = double_value
    measurement.quality.valid = is_valid
    event = definition._input_from_record(
        measurement,
        mrid.encode("utf-8"),
        kafka_timestamp_ms,
    )
    if event is None:
        raise AssertionError("Test fixture did not produce a declared input")
    return event
