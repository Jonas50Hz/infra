"""Pure stateful LFR UTC-second evaluation independent of Kafka APIs."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
import math
from typing import Any

from processor_lfr_frequency_provision.classification import worst_quality
from processor_lfr_frequency_provision.config import LfrConfig, PmuConfig
from processor_lfr_frequency_provision.selection import (
    PmuAggregate,
    PmuQuality,
    PreferredFrequency,
    select_preferred_frequency,
)


class RejectionReason(StrEnum):
    """Reasons retained for an LFR evaluation while durable audit is deferred."""

    FUTURE_TIMESTAMP = "future_timestamp"
    INVALID_STATUS = "invalid_status"
    LATE_AFTER_CLOSE = "late_after_close"
    MISSING_STATUS = "missing_status"
    MISSING_TIMESTAMP = "missing_timestamp"
    NON_FINITE_VALUE = "non_finite_value"
    OLD_DATA = "old_data"
    OPERATOR_BLOCKED = "operator_blocked"
    OUT_OF_BAND = "out_of_band"
    OVERFLOW = "overflow"
    SUBSTITUTED = "substituted"


@dataclass(frozen=True)
class QualityEvidence:
    """Current provisional mapping from Common Format generic Quality fields."""

    valid: bool | None
    substituted: bool = False
    operator_blocked: bool = False
    overflow: bool = False
    old_data: bool = False

    def rejection_reasons(self) -> tuple[RejectionReason, ...]:
        """Return every generic-quality condition that excludes a sample."""

        reasons: list[RejectionReason] = []
        if self.valid is None:
            reasons.append(RejectionReason.MISSING_STATUS)
        elif not self.valid:
            reasons.append(RejectionReason.INVALID_STATUS)
        if self.substituted:
            reasons.append(RejectionReason.SUBSTITUTED)
        if self.operator_blocked:
            reasons.append(RejectionReason.OPERATOR_BLOCKED)
        if self.overflow:
            reasons.append(RejectionReason.OVERFLOW)
        if self.old_data:
            reasons.append(RejectionReason.OLD_DATA)
        return tuple(reasons)


@dataclass(frozen=True)
class IncomingMeasurement:
    """One decoded Common Format input ready for pure LFR evaluation."""

    input_id: str
    mrid: str
    double_value: float | None
    quality: QualityEvidence
    timestamp_field_ms: int | None


@dataclass(frozen=True)
class IngestResult:
    """Outcome of recording one potential LFR source measurement."""

    accepted: bool
    ignored: bool
    reasons: tuple[RejectionReason, ...]


@dataclass(frozen=True)
class PmuSecondResult:
    """One PMU's completed result and availability evidence for a UTC second."""

    pmu_id: str
    mean_frequency_hz: float | None
    mean_voltage: float | None
    availability: float | None
    good_frequency_count: int
    total_frequency_count: int
    quality: PmuQuality


@dataclass(frozen=True)
class ClosedSecond:
    """Immutable output of evaluating one closed LFR second."""

    second: int
    closed_at_ms: int
    pmus: tuple[PmuSecondResult, ...]
    preferred_frequency: PreferredFrequency | None
    rejection_counts: Mapping[str, int]


@dataclass
class _PmuSecondState:
    frequency_values: list[float] = field(default_factory=list)
    frequency_total_count: int = 0
    rejection_counts: Counter[str] = field(default_factory=Counter)
    seen_input_ids: set[str] = field(default_factory=set)
    voltage_values: list[float] = field(default_factory=list)


class LfrSecondEngine:
    """Collect PMU values until their UTC second becomes final exactly once."""

    def __init__(self, config: LfrConfig, snapshot: Mapping[str, Any] | None = None) -> None:
        self._config = config
        self._closed_through: int | None = None
        self._states: dict[int, dict[str, _PmuSecondState]] = {}
        if snapshot is not None:
            self._restore(snapshot)

    @property
    def closed_through(self) -> int | None:
        """Return the latest UTC second that is immutable to late input."""

        return self._closed_through

    def ingest(self, measurement: IncomingMeasurement, received_at_ms: int) -> IngestResult:
        """Record one configured input, retaining every relevant rejection reason."""

        signal = self._config.signal_for(measurement.mrid)
        if signal is None:
            return IngestResult(accepted=False, ignored=True, reasons=())
        if not measurement.input_id:
            raise ValueError("input_id must not be empty")
        if measurement.timestamp_field_ms is None:
            return IngestResult(
                accepted=False,
                ignored=False,
                reasons=(RejectionReason.MISSING_TIMESTAMP,),
            )

        second = measurement.timestamp_field_ms // 1_000
        current_second = received_at_ms // 1_000
        if second > current_second + self._config.maximum_future_seconds:
            return IngestResult(
                accepted=False,
                ignored=False,
                reasons=(RejectionReason.FUTURE_TIMESTAMP,),
            )
        if self._closed_through is not None and second <= self._closed_through:
            return IngestResult(
                accepted=False,
                ignored=False,
                reasons=(RejectionReason.LATE_AFTER_CLOSE,),
            )

        state = self._state_for(second, signal.pmu)
        if measurement.input_id in state.seen_input_ids:
            return IngestResult(accepted=False, ignored=False, reasons=())
        state.seen_input_ids.add(measurement.input_id)

        reasons = self._measurement_rejection_reasons(measurement, signal.quantity)
        state.rejection_counts.update(reason.value for reason in reasons)
        if signal.quantity == "frequency":
            state.frequency_total_count += 1
            if not reasons:
                assert measurement.double_value is not None
                state.frequency_values.append(measurement.double_value)
        elif not reasons:
            assert measurement.double_value is not None
            state.voltage_values.append(measurement.double_value)
        return IngestResult(accepted=not reasons, ignored=False, reasons=reasons)

    def close_ready(self, now_ms: int) -> tuple[ClosedSecond, ...]:
        """Close every UTC second whose configured post-second deadline has passed."""

        ready_through = ((now_ms - self._config.close_delay_ms) // 1_000) - 1
        if self._closed_through is not None and ready_through <= self._closed_through:
            return ()

        closed: list[ClosedSecond] = []
        for second in sorted(value for value in self._states if value <= ready_through):
            states = self._states.pop(second)
            closed.append(self._close_second(second, now_ms, states))
        self._closed_through = ready_through
        return tuple(closed)

    def snapshot(self) -> dict[str, Any]:
        """Serialize only open evaluation state for the durable processor store."""

        return {
            "closed_through": self._closed_through,
            "states": {
                str(second): {
                    pmu_id: {
                        "frequency_values": state.frequency_values,
                        "frequency_total_count": state.frequency_total_count,
                        "rejection_counts": dict(state.rejection_counts),
                        "seen_input_ids": sorted(state.seen_input_ids),
                        "voltage_values": state.voltage_values,
                    }
                    for pmu_id, state in pmu_states.items()
                }
                for second, pmu_states in self._states.items()
            },
        }

    def _restore(self, snapshot: Mapping[str, Any]) -> None:
        closed_through = snapshot.get("closed_through")
        if closed_through is not None and (isinstance(closed_through, bool) or not isinstance(closed_through, int)):
            raise ValueError("LFR state closed_through must be an integer or null")
        raw_states = snapshot.get("states", {})
        if not isinstance(raw_states, Mapping):
            raise ValueError("LFR state states must be a mapping")

        restored: dict[int, dict[str, _PmuSecondState]] = {}
        for raw_second, raw_pmu_states in raw_states.items():
            try:
                second = int(raw_second)
            except (TypeError, ValueError) as error:
                raise ValueError("LFR state second keys must be integers") from error
            if not isinstance(raw_pmu_states, Mapping):
                raise ValueError("LFR state PMU state must be a mapping")
            restored[second] = {}
            for pmu_id, raw_state in raw_pmu_states.items():
                if not isinstance(pmu_id, str) or not isinstance(raw_state, Mapping):
                    raise ValueError("LFR state PMU entries are invalid")
                restored[second][pmu_id] = _PmuSecondState(
                    frequency_values=_finite_values(raw_state.get("frequency_values")),
                    frequency_total_count=_non_negative_int(
                        raw_state.get("frequency_total_count"),
                        "frequency_total_count",
                    ),
                    rejection_counts=Counter(_counter_mapping(raw_state.get("rejection_counts"))),
                    seen_input_ids=_string_set(raw_state.get("seen_input_ids")),
                    voltage_values=_finite_values(raw_state.get("voltage_values")),
                )
        self._closed_through = closed_through
        self._states = restored

    def _state_for(self, second: int, pmu: PmuConfig) -> _PmuSecondState:
        pmu_states = self._states.setdefault(second, {})
        return pmu_states.setdefault(pmu.pmu_id, _PmuSecondState())

    def _measurement_rejection_reasons(
        self,
        measurement: IncomingMeasurement,
        quantity: str,
    ) -> tuple[RejectionReason, ...]:
        reasons = list(measurement.quality.rejection_reasons())
        if measurement.double_value is None or not math.isfinite(measurement.double_value):
            reasons.append(RejectionReason.NON_FINITE_VALUE)
        elif quantity == "frequency" and not (
            self._config.frequency_min_hz
            <= measurement.double_value
            <= self._config.frequency_max_hz
        ):
            reasons.append(RejectionReason.OUT_OF_BAND)
        return tuple(reasons)

    def _close_second(
        self,
        second: int,
        closed_at_ms: int,
        states: Mapping[str, _PmuSecondState],
    ) -> ClosedSecond:
        results: list[PmuSecondResult] = []
        rejection_counts: Counter[str] = Counter()
        aggregates: list[PmuAggregate] = []
        for pmu in self._config.pmus:
            state = states.get(pmu.pmu_id, _PmuSecondState())
            rejection_counts.update(state.rejection_counts)
            good_count = len(state.frequency_values)
            frequency_mean = _mean_or_none(state.frequency_values)
            voltage_mean = _mean_or_none(state.voltage_values)
            availability = (
                good_count / state.frequency_total_count
                if state.frequency_total_count
                else None
            )
            count_quality = pmu.count_thresholds.classify(good_count)
            voltage_deviation = (
                voltage_mean - pmu.nominal_voltage
                if voltage_mean is not None
                else None
            )
            voltage_quality = pmu.voltage_thresholds.classify(voltage_deviation)
            quality = worst_quality(count_quality, voltage_quality)
            if frequency_mean is None:
                quality = PmuQuality.BAD
            result = PmuSecondResult(
                pmu_id=pmu.pmu_id,
                mean_frequency_hz=frequency_mean,
                mean_voltage=voltage_mean,
                availability=availability,
                good_frequency_count=good_count,
                total_frequency_count=state.frequency_total_count,
                quality=quality,
            )
            results.append(result)
            aggregates.append(
                PmuAggregate(
                    pmu_id=pmu.pmu_id,
                    mean_frequency_hz=frequency_mean,
                    quality=quality,
                )
            )
        return ClosedSecond(
            second=second,
            closed_at_ms=closed_at_ms,
            pmus=tuple(results),
            preferred_frequency=select_preferred_frequency(
                tuple(aggregates),
                self._config.even_median_tie_break,
            ),
            rejection_counts=dict(rejection_counts),
        )


def _mean_or_none(values: list[float]) -> float | None:
    if not values:
        return None
    return sum(values) / len(values)


def _finite_values(value: Any) -> list[float]:
    if not isinstance(value, list):
        raise ValueError("LFR state values must be lists")
    values: list[float] = []
    for item in value:
        if isinstance(item, bool) or not isinstance(item, int | float) or not math.isfinite(item):
            raise ValueError("LFR state values must be finite numbers")
        values.append(float(item))
    return values


def _non_negative_int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"LFR state {name} must be a non-negative integer")
    return value


def _counter_mapping(value: Any) -> Mapping[str, int]:
    if not isinstance(value, Mapping):
        raise ValueError("LFR state rejection_counts must be a mapping")
    parsed: dict[str, int] = {}
    for key, count in value.items():
        if not isinstance(key, str):
            raise ValueError("LFR state rejection count keys must be strings")
        parsed[key] = _non_negative_int(count, "rejection count")
    return parsed


def _string_set(value: Any) -> set[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) and item for item in value):
        raise ValueError("LFR state seen_input_ids must be non-empty strings")
    return set(value)