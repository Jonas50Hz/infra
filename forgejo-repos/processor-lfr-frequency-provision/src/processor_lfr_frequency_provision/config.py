"""Validated versioned configuration for LFR preferred-frequency provision."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import math
from pathlib import Path
from typing import Any

import yaml

from processor_lfr_frequency_provision.classification import (
    CountThresholds,
    VoltageThresholds,
)
from processor_lfr_frequency_provision.selection import EvenMedianTieBreak


class ConfigurationError(ValueError):
    """Raised when an LFR configuration cannot safely evaluate PMU inputs."""


@dataclass(frozen=True)
class PmuConfig:
    """Input identities and approved quality policy for one PMU."""

    pmu_id: str
    frequency_mrid: str
    voltage_mrid: str
    nominal_voltage: float
    count_thresholds: CountThresholds
    voltage_thresholds: VoltageThresholds


@dataclass(frozen=True)
class InputSignal:
    """One configured source MRID and the PMU quantity it supplies."""

    pmu: PmuConfig
    quantity: str


@dataclass(frozen=True)
class LfrConfig:
    """All engineering values required for one LFR processor deployment."""

    close_delay_ms: int
    even_median_tie_break: EvenMedianTieBreak
    frequency_max_hz: float
    frequency_min_hz: float
    maximum_future_seconds: int
    output_mrid: str
    pmus: tuple[PmuConfig, ...]
    status_evidence_mode: str
    _signals_by_mrid: Mapping[str, InputSignal] = field(
        init=False,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        if not 400 <= self.close_delay_ms <= 800:
            raise ConfigurationError("close_delay_ms must be between 400 and 800")
        if not math.isfinite(self.frequency_min_hz) or not math.isfinite(self.frequency_max_hz):
            raise ConfigurationError("frequency_band_hz must use finite values")
        if self.frequency_min_hz >= self.frequency_max_hz:
            raise ConfigurationError("frequency_band_hz.minimum must be lower than maximum")
        if self.maximum_future_seconds < 0:
            raise ConfigurationError("maximum_future_seconds must be non-negative")
        if not self.output_mrid:
            raise ConfigurationError("output_mrid must not be empty")
        if not self.pmus:
            raise ConfigurationError("pmus must contain at least one PMU")
        if self.status_evidence_mode != "generic_quality_provisional":
            raise ConfigurationError(
                "status_evidence.mode must be generic_quality_provisional until the "
                "PMU status contract is finalized"
            )

        signals: dict[str, InputSignal] = {}
        pmu_ids: set[str] = set()
        for pmu in self.pmus:
            if not pmu.pmu_id or pmu.pmu_id in pmu_ids:
                raise ConfigurationError("PMU identifiers must be unique and non-empty")
            pmu_ids.add(pmu.pmu_id)
            for mrid, quantity in (
                (pmu.frequency_mrid, "frequency"),
                (pmu.voltage_mrid, "voltage"),
            ):
                if not mrid:
                    raise ConfigurationError("configured PMU MRIDs must not be empty")
                if mrid == self.output_mrid:
                    raise ConfigurationError("output_mrid must not also be an input MRID")
                if mrid in signals:
                    raise ConfigurationError("configured PMU input MRIDs must be unique")
                signals[mrid] = InputSignal(pmu=pmu, quantity=quantity)
        object.__setattr__(self, "_signals_by_mrid", signals)

    def signal_for(self, mrid: str) -> InputSignal | None:
        """Return the configured source mapping for an incoming measurement MRID."""

        return self._signals_by_mrid.get(mrid)


def load_config(path: str | Path) -> LfrConfig:
    """Load a complete LFR deployment configuration from versioned YAML."""

    config_path = Path(path)
    try:
        with config_path.open(encoding="utf-8") as config_file:
            raw_config = yaml.safe_load(config_file)
    except OSError as error:
        raise ConfigurationError(f"Unable to read LFR configuration {config_path}: {error}") from error
    except yaml.YAMLError as error:
        raise ConfigurationError(f"Unable to parse LFR configuration {config_path}: {error}") from error

    config = _mapping(raw_config, "configuration")
    _reject_unknown_keys(
        config,
        {
            "close_delay_ms",
            "even_median_tie_break",
            "frequency_band_hz",
            "maximum_future_seconds",
            "output_mrid",
            "pmus",
            "status_evidence",
        },
        "configuration",
    )
    frequency_band = _mapping(config.get("frequency_band_hz"), "frequency_band_hz")
    _reject_unknown_keys(frequency_band, {"minimum", "maximum"}, "frequency_band_hz")
    status_evidence = _mapping(config.get("status_evidence"), "status_evidence")
    _reject_unknown_keys(status_evidence, {"mode"}, "status_evidence")

    raw_pmus = config.get("pmus")
    if not isinstance(raw_pmus, list) or not raw_pmus:
        raise ConfigurationError("pmus must be a non-empty list")

    return LfrConfig(
        close_delay_ms=_integer(config, "close_delay_ms", 400, 800),
        even_median_tie_break=_tie_break(config.get("even_median_tie_break")),
        frequency_max_hz=_finite_number(frequency_band, "maximum"),
        frequency_min_hz=_finite_number(frequency_band, "minimum"),
        maximum_future_seconds=_integer(config, "maximum_future_seconds", 0, 60),
        output_mrid=_required_string(config, "output_mrid"),
        pmus=tuple(_parse_pmu(item, index) for index, item in enumerate(raw_pmus)),
        status_evidence_mode=_required_string(status_evidence, "mode"),
    )


def _parse_pmu(raw_pmu: Any, index: int) -> PmuConfig:
    location = f"pmus[{index}]"
    pmu = _mapping(raw_pmu, location)
    _reject_unknown_keys(
        pmu,
        {
            "id",
            "frequency_mrid",
            "voltage_mrid",
            "nominal_voltage",
            "count_thresholds",
            "voltage_thresholds",
        },
        location,
    )
    count_thresholds = _mapping(pmu.get("count_thresholds"), f"{location}.count_thresholds")
    _reject_unknown_keys(
        count_thresholds,
        {"bad_maximum_good_samples", "very_good_minimum_good_samples"},
        f"{location}.count_thresholds",
    )
    voltage_thresholds = _mapping(
        pmu.get("voltage_thresholds"),
        f"{location}.voltage_thresholds",
    )
    _reject_unknown_keys(
        voltage_thresholds,
        {"very_good_maximum_deviation", "good_maximum_deviation"},
        f"{location}.voltage_thresholds",
    )
    try:
        return PmuConfig(
            pmu_id=_required_string(pmu, "id"),
            frequency_mrid=_required_string(pmu, "frequency_mrid"),
            voltage_mrid=_required_string(pmu, "voltage_mrid"),
            nominal_voltage=_positive_finite_number(pmu, "nominal_voltage"),
            count_thresholds=CountThresholds(
                bad_maximum_good_samples=_integer(
                    count_thresholds,
                    "bad_maximum_good_samples",
                    0,
                    1_000_000,
                ),
                very_good_minimum_good_samples=_integer(
                    count_thresholds,
                    "very_good_minimum_good_samples",
                    1,
                    1_000_000,
                ),
            ),
            voltage_thresholds=VoltageThresholds(
                very_good_maximum_deviation=_non_negative_finite_number(
                    voltage_thresholds,
                    "very_good_maximum_deviation",
                ),
                good_maximum_deviation=_non_negative_finite_number(
                    voltage_thresholds,
                    "good_maximum_deviation",
                ),
            ),
        )
    except ValueError as error:
        raise ConfigurationError(f"{location}: {error}") from error


def _mapping(value: Any, location: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ConfigurationError(f"{location} must be a mapping")
    return value


def _reject_unknown_keys(
    mapping: Mapping[str, Any],
    allowed_keys: set[str],
    location: str,
) -> None:
    unknown_keys = set(mapping).difference(allowed_keys)
    if unknown_keys:
        names = ", ".join(sorted(unknown_keys))
        raise ConfigurationError(f"{location} contains unsupported key(s): {names}")


def _required_string(mapping: Mapping[str, Any], name: str) -> str:
    value = mapping.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(f"{name} must be a non-empty string")
    return value.strip()


def _integer(mapping: Mapping[str, Any], name: str, minimum: int, maximum: int) -> int:
    value = mapping.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ConfigurationError(f"{name} must be an integer between {minimum} and {maximum}")
    return value


def _finite_number(mapping: Mapping[str, Any], name: str) -> float:
    value = mapping.get(name)
    if isinstance(value, bool) or not isinstance(value, int | float) or not math.isfinite(value):
        raise ConfigurationError(f"{name} must be a finite number")
    return float(value)


def _positive_finite_number(mapping: Mapping[str, Any], name: str) -> float:
    value = _finite_number(mapping, name)
    if value <= 0:
        raise ConfigurationError(f"{name} must be greater than zero")
    return value


def _non_negative_finite_number(mapping: Mapping[str, Any], name: str) -> float:
    value = _finite_number(mapping, name)
    if value < 0:
        raise ConfigurationError(f"{name} must be non-negative")
    return value


def _tie_break(value: Any) -> EvenMedianTieBreak:
    if value == "lower_frequency":
        return EvenMedianTieBreak.LOWER_FREQUENCY
    if value == "higher_frequency":
        return EvenMedianTieBreak.HIGHER_FREQUENCY
    raise ConfigurationError(
        "even_median_tie_break must be lower_frequency or higher_frequency"
    )