"""Generated adapters for constrained WAMA standard processor modes."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import logging
import math
from numbers import Real

from wama_processor.definition import (
    DerivedMeasurement,
    InputMeasurement,
    ProcessorDefinition,
)


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class LatestValuesGroup:
    """One output calculation and the eligible cached inputs it requires."""

    output: str
    inputs: tuple[str, ...]
    maximum_age_ms: int


def build_formula_processor(
    *,
    service_name: str,
    inputs: Mapping[str, str],
    outputs: Mapping[str, str],
    input_name: str,
    output_name: str,
    calculation: Callable[..., float],
) -> ProcessorDefinition:
    """Build a standard single-input formula adapter around a pure calculation."""

    if set(inputs) != {input_name} or set(outputs) != {output_name}:
        raise ValueError("formula mode requires exactly one declared input and output")
    if not callable(calculation):
        raise ValueError("formula calculation must be callable")

    def transform(measurement: InputMeasurement) -> DerivedMeasurement | None:
        if measurement.name != input_name:
            return None
        value = _eligible_double(measurement)
        if value is None:
            return None
        result = _calculate(calculation, output_name, {input_name: value})
        if result is None:
            return None
        return measurement.derive(output_name, result)

    return ProcessorDefinition(
        service_name=service_name,
        inputs=inputs,
        outputs=outputs,
        transform=transform,
    )


def build_latest_values_processor(
    *,
    service_name: str,
    inputs: Mapping[str, str],
    outputs: Mapping[str, str],
    groups: tuple[LatestValuesGroup, ...],
    calculations: Mapping[str, Callable[..., float]],
) -> ProcessorDefinition:
    """Build an ephemeral standard latest-values processor with bounded freshness."""

    adapter = _LatestValuesAdapter(inputs, outputs, groups, calculations)
    return ProcessorDefinition(
        service_name=service_name,
        inputs=inputs,
        outputs=outputs,
        transform=adapter.transform,
    )


class _LatestValuesAdapter:
    """Keep declared valid values only until their group can safely emit."""

    def __init__(
        self,
        inputs: Mapping[str, str],
        outputs: Mapping[str, str],
        groups: tuple[LatestValuesGroup, ...],
        calculations: Mapping[str, Callable[..., float]],
    ) -> None:
        if not groups:
            raise ValueError("latest-values mode requires at least one group")
        if set(calculations) != set(outputs):
            raise ValueError("latest-values calculations must match declared outputs")
        if not all(callable(calculation) for calculation in calculations.values()):
            raise ValueError("latest-values calculations must be callable")

        groups_by_input: dict[str, LatestValuesGroup] = {}
        group_outputs: set[str] = set()
        for group in groups:
            if group.output not in outputs or group.output in group_outputs:
                raise ValueError("latest-values groups must name each declared output once")
            if group.maximum_age_ms <= 0 or len(group.inputs) < 2:
                raise ValueError("latest-values groups require a positive age and two inputs")
            for input_name in group.inputs:
                if input_name not in inputs or input_name in groups_by_input:
                    raise ValueError("latest-values inputs must belong to exactly one group")
                groups_by_input[input_name] = group
            group_outputs.add(group.output)
        if set(groups_by_input) != set(inputs) or group_outputs != set(outputs):
            raise ValueError("latest-values groups must cover all declared inputs and outputs")

        self._calculations = dict(calculations)
        self._groups_by_input = groups_by_input
        self._values: dict[str, InputMeasurement] = {}

    def transform(self, measurement: InputMeasurement) -> DerivedMeasurement | None:
        """Cache one eligible input and emit only a fresh, complete group."""

        group = self._groups_by_input.get(measurement.name)
        if group is None:
            return None
        if _eligible_double(measurement) is None:
            return None
        self._values[measurement.name] = measurement
        if any(input_name not in self._values for input_name in group.inputs):
            return None

        group_values = {input_name: self._values[input_name] for input_name in group.inputs}
        newest_timestamp_ms = max(
            value.kafka_timestamp_ms for value in group_values.values()
        )
        stale_inputs = [
            input_name
            for input_name, value in group_values.items()
            if newest_timestamp_ms - value.kafka_timestamp_ms > group.maximum_age_ms
        ]
        if stale_inputs:
            for input_name in stale_inputs:
                del self._values[input_name]
            return None

        values = {
            input_name: float(value.double_value)
            for input_name, value in group_values.items()
            if value.double_value is not None
        }
        result = _calculate(self._calculations[group.output], group.output, values)
        if result is None:
            return None
        return measurement.derive(
            group.output,
            result,
            kafka_timestamp_ms=newest_timestamp_ms,
        )


def _eligible_double(measurement: InputMeasurement) -> float | None:
    if not measurement.is_valid or measurement.double_value is None:
        return None
    value = measurement.double_value
    if isinstance(value, bool) or not isinstance(value, Real):
        return None
    numeric_value = float(value)
    return numeric_value if math.isfinite(numeric_value) else None


def _calculate(
    calculation: Callable[..., float],
    output_name: str,
    values: Mapping[str, float],
) -> float | None:
    try:
        result = calculation(**values)
    except Exception as error:
        LOGGER.warning("Calculation for %s failed: %s", output_name, type(error).__name__)
        return None
    if isinstance(result, bool) or not isinstance(result, Real):
        LOGGER.warning("Calculation for %s returned a non-numeric result", output_name)
        return None
    numeric_result = float(result)
    if not math.isfinite(numeric_result):
        LOGGER.warning("Calculation for %s returned a non-finite result", output_name)
        return None
    return numeric_result