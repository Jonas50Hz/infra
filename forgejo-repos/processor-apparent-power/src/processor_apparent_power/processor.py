"""Calculate apparent power from the latest valid voltage/current phase pair."""

from __future__ import annotations

from wama_processor import DerivedMeasurement, InputMeasurement, ProcessorDefinition

SOURCE_PREFIX = "urn:wama:poc:pmu:bay-01"
PHASES = ("l1", "l2", "l3")
INPUT_IDENTITIES = {
    f"voltage_{phase}": (phase, "voltage")
    for phase in PHASES
} | {
    f"current_{phase}": (phase, "current")
    for phase in PHASES
}
INPUTS = {
    name: f"{SOURCE_PREFIX}:{quantity}-{phase}"
    for name, (phase, quantity) in INPUT_IDENTITIES.items()
}
OUTPUTS = {
    f"apparent_power_{phase}": f"{SOURCE_PREFIX}:apparent-power-{phase}"
    for phase in PHASES
}


class PhaseCache:
    """Keep the latest explicitly valid voltage and current for each PMU phase."""

    def __init__(self) -> None:
        self._measurements: dict[str, dict[str, InputMeasurement]] = {
            phase: {} for phase in PHASES
        }

    def transform(self, measurement: InputMeasurement) -> DerivedMeasurement | None:
        """Cache one valid input and emit its phase power when the pair is complete."""

        identity = INPUT_IDENTITIES.get(measurement.name)
        if identity is None or measurement.double_value is None or not measurement.is_valid:
            return None

        phase, quantity = identity
        phase_measurements = self._measurements[phase]
        phase_measurements[quantity] = measurement
        voltage = phase_measurements.get("voltage")
        current = phase_measurements.get("current")
        if voltage is None or current is None:
            return None

        assert voltage.double_value is not None
        assert current.double_value is not None
        return measurement.derive(
            f"apparent_power_{phase}",
            voltage.double_value * current.double_value,
        )


def build_processor(cache: PhaseCache | None = None) -> ProcessorDefinition:
    """Create a processor definition with an isolated phase cache when needed."""

    phase_cache = PhaseCache() if cache is None else cache
    return ProcessorDefinition(
        service_name="processor-apparent-power",
        inputs=INPUTS,
        outputs=OUTPUTS,
        transform=phase_cache.transform,
    )


PROCESSOR = build_processor()
