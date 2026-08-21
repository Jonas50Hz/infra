"""Generated latest-values adapter for the apparent-power declaration."""

from __future__ import annotations

from wama_processor import (
    LatestValuesGroup,
    ProcessorDefinition,
    build_latest_values_processor,
)

from processor_apparent_power.calculation import (
    apparent_power_l1,
    apparent_power_l2,
    apparent_power_l3,
)

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
GROUPS = tuple(
    LatestValuesGroup(
        output=f"apparent_power_{phase}",
        inputs=(f"voltage_{phase}", f"current_{phase}"),
        maximum_age_ms=2_000,
    )
    for phase in PHASES
)
CALCULATIONS = {
    "apparent_power_l1": apparent_power_l1,
    "apparent_power_l2": apparent_power_l2,
    "apparent_power_l3": apparent_power_l3,
}


def build_processor() -> ProcessorDefinition:
    """Create an isolated ephemeral cache for one processor runtime instance."""

    return build_latest_values_processor(
        service_name="processor-apparent-power",
        inputs=INPUTS,
        outputs=OUTPUTS,
        groups=GROUPS,
        calculations=CALCULATIONS,
    )


PROCESSOR = build_processor()
