"""Generated formula adapter for the frequency-scale authoring declaration."""

from __future__ import annotations

from wama_processor import ProcessorDefinition, build_formula_processor

from processor_frequency_scale.calculation import HERTZ_TO_MILLIHERTZ, frequency_millihertz


FREQUENCY_HZ = "frequency_hz"
FREQUENCY_MILLIHERTZ = "frequency_millihertz"
INPUTS = {
    FREQUENCY_HZ: "urn:wama:poc:pmu:bay-01:frequency",
}
OUTPUTS = {
    FREQUENCY_MILLIHERTZ: "urn:wama:poc:pmu:bay-01:frequency-millihertz",
}
PROCESSOR: ProcessorDefinition = build_formula_processor(
    service_name="processor-frequency-scale",
    inputs=INPUTS,
    outputs=OUTPUTS,
    input_name=FREQUENCY_HZ,
    output_name=FREQUENCY_MILLIHERTZ,
    calculation=frequency_millihertz,
)

# This alias is retained for domain-focused tests; runtime mechanics remain generated.
transform = PROCESSOR.transform
