"""In-process apparent-power calculation for the fake PMU phase pairs."""

from __future__ import annotations

from dataclasses import dataclass

from processor_apparent_power.generated.rtd_schema_pb2 import MCCSMeasurementValue

SOURCE_PREFIX = "urn:wama:poc:pmu:bay-01"
PHASES = ("l1", "l2", "l3")
SOURCE_IDENTITIES = {
    f"{SOURCE_PREFIX}:voltage-{phase}": (phase, "voltage")
    for phase in PHASES
} | {
    f"{SOURCE_PREFIX}:current-{phase}": (phase, "current")
    for phase in PHASES
}
OUTPUT_MRIDS = {
    phase: f"{SOURCE_PREFIX}:apparent-power-{phase}"
    for phase in PHASES
}


@dataclass(frozen=True)
class DerivedMeasurement:
    """A derived measurement with its Kafka timestamp to publish."""

    measurement: MCCSMeasurementValue
    kafka_timestamp_ms: int


class PhaseCache:
    """Keep the latest explicitly valid voltage and current for each PMU phase."""

    def __init__(self) -> None:
        self._measurements: dict[str, dict[str, MCCSMeasurementValue]] = {
            phase: {} for phase in PHASES
        }

    def transform(
        self,
        measurement: MCCSMeasurementValue,
        key: bytes | None,
        kafka_timestamp_ms: int,
        _headers: object = None,
    ) -> DerivedMeasurement | None:
        """Cache one valid source and emit its phase power when the pair is complete."""

        source_identity = SOURCE_IDENTITIES.get(measurement.mrid)
        if source_identity is None or key != measurement.mrid.encode("utf-8"):
            return None
        if measurement.WhichOneof("value") != "double_value":
            return None
        if not _is_explicitly_valid(measurement):
            return None

        phase, quantity = source_identity
        phase_measurements = self._measurements[phase]
        phase_measurements[quantity] = _copy_measurement(measurement)
        voltage = phase_measurements.get("voltage")
        current = phase_measurements.get("current")
        if voltage is None or current is None:
            return None

        derived = _copy_measurement(measurement)
        derived.mrid = OUTPUT_MRIDS[phase]
        derived.double_value = voltage.double_value * current.double_value
        return DerivedMeasurement(
            measurement=derived,
            kafka_timestamp_ms=kafka_timestamp_ms,
        )


def output_key(measurement: MCCSMeasurementValue) -> bytes:
    """Return the deterministic Kafka key for a derived apparent-power value."""

    return measurement.mrid.encode("utf-8")


def preserve_kafka_timestamp(
    derived: DerivedMeasurement,
    _key: bytes | None,
    _timestamp: int,
    _headers: object,
) -> int:
    """Publish with the triggering source record's Kafka timestamp."""

    return derived.kafka_timestamp_ms


def _is_explicitly_valid(measurement: MCCSMeasurementValue) -> bool:
    return (
        measurement.HasField("quality")
        and measurement.quality.HasField("valid")
        and measurement.quality.valid
    )


def _copy_measurement(measurement: MCCSMeasurementValue) -> MCCSMeasurementValue:
    copied = MCCSMeasurementValue()
    copied.CopyFrom(measurement)
    return copied