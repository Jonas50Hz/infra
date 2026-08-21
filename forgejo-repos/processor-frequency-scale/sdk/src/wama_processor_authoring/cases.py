"""Executable engineering examples for standard WAMA processor modes."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import yaml

from wama_processor_authoring.catalog import ResolvedProcessor
from wama_processor_authoring.errors import AuthoringValidationError
from wama_processor_authoring.simulation import (
    InputSample,
    LatestValuesSimulator,
    SimulationResult,
    simulate_formula,
)


@dataclass(frozen=True)
class CaseResult:
    """The outcome of one named engineering case."""

    name: str
    passed: bool
    actual: dict[str, object]
    expected: dict[str, object] | str


def load_cases(path: Path) -> tuple[Mapping[str, object], ...]:
    """Load a non-empty ordered list of engineering cases."""

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise AuthoringValidationError(f"Unable to read cases: {path}") from error
    except yaml.YAMLError as error:
        raise AuthoringValidationError(f"Invalid YAML in cases: {path}") from error
    if not isinstance(raw, Mapping):
        raise AuthoringValidationError("cases document must be a mapping")
    entries = raw.get("cases")
    if not isinstance(entries, list) or not entries:
        raise AuthoringValidationError("cases must be a non-empty list")
    cases: list[Mapping[str, object]] = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, Mapping):
            raise AuthoringValidationError(f"cases[{index}] must be a mapping")
        cases.append(entry)
    return tuple(cases)


def run_cases(
    processor: ResolvedProcessor,
    calculations: Mapping[str, Callable[..., float]],
    cases: tuple[Mapping[str, object], ...],
) -> tuple[CaseResult, ...]:
    """Run cases with the same local adapter policies used by standard runtime."""

    if processor.manifest.kind == "formula":
        return tuple(_run_formula_case(processor, calculations, case, index) for index, case in enumerate(cases))
    if processor.manifest.kind == "latest-values":
        return tuple(
            _run_latest_values_case(processor, calculations, case, index)
            for index, case in enumerate(cases)
        )
    raise AuthoringValidationError("engineering cases are available only for standard processor modes")


def report_cases(results: tuple[CaseResult, ...]) -> dict[str, object]:
    """Return a machine-readable report appropriate for CI build evidence."""

    return {
        "passed": all(result.passed for result in results),
        "cases": [asdict(result) for result in results],
    }


def _run_formula_case(
    processor: ResolvedProcessor,
    calculations: Mapping[str, Callable[..., float]],
    case: Mapping[str, object],
    index: int,
) -> CaseResult:
    name = _case_name(case, index)
    output_name = next(iter(processor.manifest.outputs))
    calculation = calculations.get(output_name)
    if calculation is None:
        raise AuthoringValidationError(f"formula calculation {output_name!r} is unavailable")
    inputs = _sample_mapping(case.get("inputs"), f"cases[{index}].inputs")
    result = simulate_formula(processor, calculation, inputs)
    return _case_result(name, result, case.get("expect"), f"cases[{index}].expect")


def _run_latest_values_case(
    processor: ResolvedProcessor,
    calculations: Mapping[str, Callable[..., float]],
    case: Mapping[str, object],
    index: int,
) -> CaseResult:
    name = _case_name(case, index)
    simulator = LatestValuesSimulator(processor, calculations)
    events = _ordered_samples(case.get("inputs"), f"cases[{index}].inputs")
    result = SimulationResult(output=None, reason="no_input")
    for input_name, sample in events:
        result = simulator.accept(input_name, sample)
    return _case_result(name, result, case.get("expect"), f"cases[{index}].expect")


def _case_result(
    name: str,
    result: SimulationResult,
    expected: object,
    location: str,
) -> CaseResult:
    actual = _result_payload(result)
    expected_payload = _expected_payload(expected, location)
    return CaseResult(
        name=name,
        passed=_matches_expected(actual, expected_payload),
        actual=actual,
        expected=expected_payload,
    )


def _sample_mapping(raw: object, location: str) -> dict[str, InputSample]:
    if not isinstance(raw, Mapping):
        raise AuthoringValidationError(f"{location} must be a mapping")
    return {name: _sample(sample, f"{location}.{name}") for name, sample in raw.items()}


def _ordered_samples(raw: object, location: str) -> tuple[tuple[str, InputSample], ...]:
    if isinstance(raw, Mapping):
        return tuple((name, _sample(sample, f"{location}.{name}")) for name, sample in raw.items())
    if not isinstance(raw, list) or not raw:
        raise AuthoringValidationError(f"{location} must be an ordered mapping or non-empty list")
    events: list[tuple[str, InputSample]] = []
    for index, raw_event in enumerate(raw):
        if not isinstance(raw_event, Mapping):
            raise AuthoringValidationError(f"{location}[{index}] must be a mapping")
        input_name = raw_event.get("input")
        if not isinstance(input_name, str) or not input_name:
            raise AuthoringValidationError(f"{location}[{index}].input must be a non-empty string")
        sample = {key: value for key, value in raw_event.items() if key != "input"}
        events.append((input_name, _sample(sample, f"{location}[{index}]")))
    return tuple(events)


def _sample(raw: object, location: str) -> InputSample:
    if not isinstance(raw, Mapping):
        raise AuthoringValidationError(f"{location} must be a mapping")
    value = raw.get("value")
    valid = raw.get("valid")
    timestamp_ms = raw.get("timestamp_ms")
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise AuthoringValidationError(f"{location}.value must be a number")
    if not isinstance(valid, bool):
        raise AuthoringValidationError(f"{location}.valid must be a boolean")
    if isinstance(timestamp_ms, bool) or not isinstance(timestamp_ms, int):
        raise AuthoringValidationError(f"{location}.timestamp_ms must be an integer")
    return InputSample(value=float(value), valid=valid, timestamp_ms=timestamp_ms)


def _case_name(case: Mapping[str, object], index: int) -> str:
    name = case.get("name")
    if not isinstance(name, str) or not name.strip():
        raise AuthoringValidationError(f"cases[{index}].name must be a non-empty string")
    return name.strip()


def _result_payload(result: SimulationResult) -> dict[str, object]:
    if result.output is None:
        return {"reason": result.reason}
    return {
        "output": result.output.name,
        "value": result.output.value,
        "unit": result.output.unit,
        "timestamp_ms": result.output.timestamp_ms,
    }


def _expected_payload(expected: object, location: str) -> dict[str, object] | str:
    if expected == "no_output":
        return "no_output"
    if not isinstance(expected, Mapping) or len(expected) != 1:
        raise AuthoringValidationError(
            f"{location} must be 'no_output' or one expected output mapping"
        )
    output_name, raw_output = next(iter(expected.items()))
    if not isinstance(output_name, str) or not isinstance(raw_output, Mapping):
        raise AuthoringValidationError(f"{location} must name one output mapping")
    value = raw_output.get("value")
    unit = raw_output.get("unit")
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise AuthoringValidationError(f"{location}.{output_name}.value must be a number")
    if not isinstance(unit, str) or not unit:
        raise AuthoringValidationError(f"{location}.{output_name}.unit must be a non-empty string")
    expected_payload: dict[str, object] = {
        "output": output_name,
        "value": float(value),
        "unit": unit,
    }
    if "timestamp_ms" in raw_output:
        timestamp_ms = raw_output["timestamp_ms"]
        if isinstance(timestamp_ms, bool) or not isinstance(timestamp_ms, int):
            raise AuthoringValidationError(
                f"{location}.{output_name}.timestamp_ms must be an integer"
            )
        expected_payload["timestamp_ms"] = timestamp_ms
    return expected_payload


def _matches_expected(actual: Mapping[str, object], expected: dict[str, object] | str) -> bool:
    if expected == "no_output":
        return "output" not in actual
    if actual.get("output") != expected["output"] or actual.get("unit") != expected["unit"]:
        return False
    if abs(float(actual["value"]) - float(expected["value"])) > 1e-9:
        return False
    return "timestamp_ms" not in expected or actual.get("timestamp_ms") == expected["timestamp_ms"]