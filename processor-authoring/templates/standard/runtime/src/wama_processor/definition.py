"""Domain-facing processor declarations independent of Kafka and Protobuf APIs."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from numbers import Real

from wama_processor.generated.rtd_schema_pb2 import MCCSMeasurementValue


class ProcessorDefinitionError(ValueError):
    """Raised when a processor declaration cannot safely run on LiveMeasurement."""


@dataclass(frozen=True)
class DerivedMeasurement:
    """A declared derived value ready for the framework to publish."""

    name: str
    double_value: float
    _measurement: MCCSMeasurementValue
    kafka_timestamp_ms: int

    @property
    def protobuf(self) -> MCCSMeasurementValue:
        """Return the framework-owned Common Format record for publication."""

        return self._measurement


@dataclass(frozen=True)
class InputMeasurement:
    """One named measurement an EE can inspect and use to derive an output."""

    name: str
    double_value: float | None
    is_valid: bool
    _source: MCCSMeasurementValue
    _output_mrids: Mapping[str, str]
    kafka_timestamp_ms: int

    def derive(
        self,
        output_name: str,
        double_value: float,
        *,
        kafka_timestamp_ms: int | None = None,
    ) -> DerivedMeasurement:
        """Create a declared double-valued output with the source context intact."""

        output_mrid = self._output_mrids.get(output_name)
        if output_mrid is None:
            raise ProcessorDefinitionError(
                f"{self.name} cannot derive undeclared output {output_name!r}"
            )
        if isinstance(double_value, bool) or not isinstance(double_value, Real):
            raise ProcessorDefinitionError("Derived double_value must be a number")
        if kafka_timestamp_ms is not None and (
            isinstance(kafka_timestamp_ms, bool) or not isinstance(kafka_timestamp_ms, int)
        ):
            raise ProcessorDefinitionError("Derived Kafka timestamp must be an integer")

        derived = MCCSMeasurementValue()
        derived.CopyFrom(self._source)
        derived.mrid = output_mrid
        derived.double_value = float(double_value)
        return DerivedMeasurement(
            name=output_name,
            double_value=float(double_value),
            _measurement=derived,
            kafka_timestamp_ms=(
                self.kafka_timestamp_ms if kafka_timestamp_ms is None else kafka_timestamp_ms
            ),
        )


@dataclass(frozen=True)
class ProcessorDefinition:
    """Declare a processor's named inputs, outputs, and electrical calculation."""

    service_name: str
    inputs: Mapping[str, str]
    outputs: Mapping[str, str]
    transform: Callable[[InputMeasurement], DerivedMeasurement | None]
    _input_names_by_mrid: Mapping[str, str] = field(
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if not isinstance(self.service_name, str) or not self.service_name.strip():
            raise ProcessorDefinitionError("service_name must be a non-empty string")
        if not callable(self.transform):
            raise ProcessorDefinitionError("transform must be callable")

        inputs = _validated_signals(self.inputs, "inputs")
        outputs = _validated_signals(self.outputs, "outputs")
        shared_mrids = set(inputs.values()).intersection(outputs.values())
        if shared_mrids:
            names = ", ".join(sorted(shared_mrids))
            raise ProcessorDefinitionError(
                f"inputs and outputs must use distinct MRIDs: {names}"
            )

        object.__setattr__(self, "inputs", inputs)
        object.__setattr__(self, "outputs", outputs)
        object.__setattr__(
            self,
            "_input_names_by_mrid",
            {mrid: name for name, mrid in inputs.items()},
        )

    def transform_record(
        self,
        measurement: MCCSMeasurementValue,
        key: bytes | None,
        kafka_timestamp_ms: int,
    ) -> DerivedMeasurement | None:
        """Safely adapt one Common Format record to the author transform."""

        input_measurement = self._input_from_record(
            measurement,
            key,
            kafka_timestamp_ms,
        )
        if input_measurement is None:
            return None

        derived = self.transform(input_measurement)
        if derived is None:
            return None
        if not isinstance(derived, DerivedMeasurement):
            raise ProcessorDefinitionError("transform must return a derived measurement or None")
        expected_mrid = self.outputs.get(derived.name)
        if expected_mrid is None or derived.protobuf.mrid != expected_mrid:
            raise ProcessorDefinitionError("transform must derive a declared output")
        if derived.kafka_timestamp_ms != kafka_timestamp_ms:
            raise ProcessorDefinitionError(
                "transform must preserve the triggering Kafka timestamp"
            )
        return derived

    def _input_from_record(
        self,
        measurement: MCCSMeasurementValue,
        key: bytes | None,
        kafka_timestamp_ms: int,
    ) -> InputMeasurement | None:
        name = self._input_names_by_mrid.get(measurement.mrid)
        if name is None or key != measurement.mrid.encode("utf-8"):
            return None

        source = MCCSMeasurementValue()
        source.CopyFrom(measurement)
        double_value = (
            source.double_value
            if source.WhichOneof("value") == "double_value"
            else None
        )
        return InputMeasurement(
            name=name,
            double_value=double_value,
            is_valid=_is_explicitly_valid(source),
            _source=source,
            _output_mrids=self.outputs,
            kafka_timestamp_ms=kafka_timestamp_ms,
        )


def _validated_signals(signals: Mapping[str, str], location: str) -> dict[str, str]:
    if not isinstance(signals, Mapping):
        raise ProcessorDefinitionError(f"{location} must be a mapping")

    validated: dict[str, str] = {}
    for name, mrid in signals.items():
        if not isinstance(name, str) or not name.strip():
            raise ProcessorDefinitionError(f"{location} signal names must be non-empty strings")
        if not isinstance(mrid, str) or not mrid.strip():
            raise ProcessorDefinitionError(
                f"{location}.{name} must use a non-empty MRID string"
            )
        validated[name] = mrid

    if len(set(validated.values())) != len(validated):
        raise ProcessorDefinitionError(f"{location} MRIDs must be unique")
    return validated


def _is_explicitly_valid(measurement: MCCSMeasurementValue) -> bool:
    return (
        measurement.HasField("quality")
        and measurement.quality.HasField("valid")
        and measurement.quality.valid
    )
