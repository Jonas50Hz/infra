"""Kafka-free engineering simulation for constrained standard processor modes."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import math
from numbers import Real

from wama_processor_authoring.catalog import ResolvedProcessor
from wama_processor_authoring.errors import AuthoringValidationError


@dataclass(frozen=True)
class InputSample:
    """One named engineering value provided to the local simulator."""

    value: float
    valid: bool
    timestamp_ms: int


@dataclass(frozen=True)
class SimulatedOutput:
    """One standard-mode result expressed in engineering units."""

    name: str
    value: float
    unit: str
    timestamp_ms: int


@dataclass(frozen=True)
class SimulationResult:
    """A simulation outcome, including why an output was suppressed."""

    output: SimulatedOutput | None
    reason: str | None


class LatestValuesSimulator:
    """Ephemeral latest-values state using the standard runtime eligibility rules."""

    def __init__(
        self,
        processor: ResolvedProcessor,
        calculations: Mapping[str, Callable[..., float]],
    ) -> None:
        """Create an isolated cache for one processor-instance simulation."""

        if processor.manifest.kind != "latest-values":
            raise AuthoringValidationError(
                "LatestValuesSimulator requires a latest-values processor"
            )
        expected_outputs = set(processor.manifest.outputs)
        if set(calculations) != expected_outputs:
            raise AuthoringValidationError(
                "latest-values calculations must match the declared outputs exactly"
            )
        if not all(callable(calculation) for calculation in calculations.values()):
            raise AuthoringValidationError("latest-values calculations must be callable")

        self._processor = processor
        self._calculations = dict(calculations)
        self._values: dict[str, InputSample] = {}
        self._groups_by_input = {
            input_name: group
            for group in processor.manifest.latest_values_groups
            for input_name in group.inputs
        }

    def accept(self, input_name: str, sample: InputSample) -> SimulationResult:
        """Apply one input in arrival order and maybe emit its completed output."""

        group = self._groups_by_input.get(input_name)
        if group is None:
            raise AuthoringValidationError(f"unknown latest-values input {input_name!r}")
        reason = _ineligible_input_reason(input_name, sample)
        if reason is not None:
            return SimulationResult(output=None, reason=reason)

        self._values[input_name] = sample
        missing = [name for name in group.inputs if name not in self._values]
        if missing:
            return SimulationResult(output=None, reason=f"incomplete_group:{group.output}")

        group_samples = {name: self._values[name] for name in group.inputs}
        newest_timestamp = max(item.timestamp_ms for item in group_samples.values())
        stale_inputs = [
            name
            for name, item in group_samples.items()
            if newest_timestamp - item.timestamp_ms > group.maximum_age_ms
        ]
        if stale_inputs:
            for stale_input in stale_inputs:
                del self._values[stale_input]
            return SimulationResult(
                output=None,
                reason=f"stale_input:{','.join(sorted(stale_inputs))}",
            )

        try:
            result = self._calculations[group.output](
                **{name: float(item.value) for name, item in group_samples.items()}
            )
        except Exception as error:
            return SimulationResult(output=None, reason=f"calculation_error:{type(error).__name__}")
        if isinstance(result, bool) or not isinstance(result, Real):
            return SimulationResult(output=None, reason="result_not_numeric")
        numeric_result = float(result)
        if not math.isfinite(numeric_result):
            return SimulationResult(output=None, reason="non_finite_result")

        output = self._processor.manifest.outputs[group.output]
        return SimulationResult(
            output=SimulatedOutput(
                name=output.name,
                value=numeric_result,
                unit=output.unit,
                timestamp_ms=newest_timestamp,
            ),
            reason=None,
        )


def simulate_formula(
    processor: ResolvedProcessor,
    calculation: Callable[..., float],
    inputs: Mapping[str, InputSample],
) -> SimulationResult:
    """Evaluate a formula using the same validity and finite-result rules as runtime."""

    manifest = processor.manifest
    if manifest.kind != "formula":
        raise AuthoringValidationError("simulate_formula requires a formula processor")
    if not callable(calculation):
        raise AuthoringValidationError("formula calculation must be callable")

    values: dict[str, float] = {}
    timestamps: list[int] = []
    for name in manifest.inputs:
        sample = inputs.get(name)
        if sample is None:
            return SimulationResult(output=None, reason=f"missing_input:{name}")
        reason = _ineligible_input_reason(name, sample)
        if reason is not None:
            return SimulationResult(output=None, reason=reason)
        value = float(sample.value)
        values[name] = value
        timestamps.append(sample.timestamp_ms)

    try:
        result = calculation(**values)
    except Exception as error:
        return SimulationResult(output=None, reason=f"calculation_error:{type(error).__name__}")
    if isinstance(result, bool) or not isinstance(result, Real):
        return SimulationResult(output=None, reason="result_not_numeric")
    numeric_result = float(result)
    if not math.isfinite(numeric_result):
        return SimulationResult(output=None, reason="non_finite_result")

    output = next(iter(manifest.outputs.values()))
    return SimulationResult(
        output=SimulatedOutput(
            name=output.name,
            value=numeric_result,
            unit=output.unit,
            timestamp_ms=max(timestamps),
        ),
        reason=None,
    )


def _ineligible_input_reason(input_name: str, sample: InputSample) -> str | None:
    if not sample.valid:
        return f"invalid_quality:{input_name}"
    if isinstance(sample.value, bool) or not isinstance(sample.value, Real):
        return f"scalar_mismatch:{input_name}"
    if not math.isfinite(float(sample.value)):
        return f"non_finite_input:{input_name}"
    if isinstance(sample.timestamp_ms, bool) or not isinstance(sample.timestamp_ms, int):
        raise AuthoringValidationError(f"input timestamp for {input_name} must be an integer")
    return None