"""Convert the fake PMU frequency from hertz to millihertz."""

from __future__ import annotations

from wama_processor import DerivedMeasurement, InputMeasurement, ProcessorDefinition

FREQUENCY_HZ = "frequency_hz"
FREQUENCY_MILLIHERTZ = "frequency_millihertz"
INPUTS = {
    FREQUENCY_HZ: "urn:wama:poc:pmu:bay-01:frequency",
}
OUTPUTS = {
    FREQUENCY_MILLIHERTZ: "urn:wama:poc:pmu:bay-01:frequency-millihertz",
}
HERTZ_TO_MILLIHERTZ = 1_000.0


def transform(measurement: InputMeasurement) -> DerivedMeasurement | None:
    """Convert one numeric frequency measurement without changing its context."""

    if measurement.name != FREQUENCY_HZ or measurement.double_value is None:
        return None
    return measurement.derive(
        FREQUENCY_MILLIHERTZ,
        measurement.double_value * HERTZ_TO_MILLIHERTZ,
    )


PROCESSOR = ProcessorDefinition(
    service_name="processor-frequency-scale",
    inputs=INPUTS,
    outputs=OUTPUTS,
    transform=transform,
)
