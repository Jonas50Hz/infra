"""Normalize decoded C37.118 v2 data into WAMA Common Format values."""

from __future__ import annotations

from dataclasses import dataclass
import math

from google.protobuf.timestamp_pb2 import Timestamp

from gateway_c37_118_onboarding.c37_118_v2 import (
    ConfigurationFrame,
    DataFrame,
    PmuConfiguration,
    WIRE_VERSION,
)
from gateway_c37_118_onboarding.config import SignalDefinition, SourceDefinition
from gateway_c37_118_onboarding.generated import rtd_schema_pb2


class NormalizationError(ValueError):
    """Raised when a source configuration or decoded frame cannot be normalized safely."""


@dataclass(frozen=True)
class SignalBinding:
    """One validated catalog signal bound to a value in a configured PMU block."""

    signal: SignalDefinition
    selector_kind: str
    phasor_index: int | None = None


@dataclass(frozen=True)
class SourceMapping:
    """A source-specific mapping derived from one accepted CFG-2 frame."""

    source: SourceDefinition
    configuration: ConfigurationFrame
    time_base: int
    pmu_idcode: int
    configuration_count: int
    bindings: tuple[SignalBinding, ...]


def build_source_mapping(
    source: SourceDefinition,
    configuration: ConfigurationFrame,
) -> SourceMapping:
    """Bind a reviewed source catalog entry to the matching C37.118 CFG-2 layout."""

    if source.wire_version != WIRE_VERSION:
        raise NormalizationError("Source does not use the supported C37.118 wire version")
    pmu = configuration.pmu(source.pmu_idcode)
    bindings = tuple(_bind_signal(signal, pmu) for signal in source.signals)
    return SourceMapping(
        source=source,
        configuration=configuration,
        time_base=configuration.time_base,
        pmu_idcode=pmu.idcode,
        configuration_count=pmu.configuration_count,
        bindings=bindings,
    )


def normalize_data_frame(
    mapping: SourceMapping,
    data_frame: DataFrame,
    gateway_timestamp_ms: int,
) -> tuple[rtd_schema_pb2.MCCSMeasurementValue, ...]:
    """Create one validated Common Format record per mapped source signal."""

    if isinstance(gateway_timestamp_ms, bool) or gateway_timestamp_ms < 0:
        raise NormalizationError("gateway_timestamp_ms must be a non-negative integer")
    field_timestamp = _field_timestamp(data_frame.header.soc, data_frame.header.fracsec, mapping.time_base)
    gateway_timestamp = _epoch_milliseconds_timestamp(gateway_timestamp_ms)
    if _timestamp_nanoseconds(field_timestamp) > _timestamp_nanoseconds(gateway_timestamp):
        raise NormalizationError("C37.118 field timestamp is later than gateway receipt time")

    pmu_data = data_frame.pmu(mapping.pmu_idcode)
    measurements: list[rtd_schema_pb2.MCCSMeasurementValue] = []
    for binding in mapping.bindings:
        value = _binding_value(binding, pmu_data.phasor_magnitudes, pmu_data.frequency_hz, pmu_data.rocof_hz_per_s)
        if not math.isfinite(value):
            raise NormalizationError("C37.118 value is not finite")
        measurement = rtd_schema_pb2.MCCSMeasurementValue(mrid=binding.signal.mrid)
        measurement.double_value = value
        measurement.timestamp_field.CopyFrom(field_timestamp)
        measurement.timestamp_gateway.CopyFrom(gateway_timestamp)
        measurement.timestamp_mccs.CopyFrom(gateway_timestamp)
        _apply_v2_quality(measurement, pmu_data.stat)
        measurements.append(measurement)
    return tuple(measurements)


def _bind_signal(signal: SignalDefinition, pmu: PmuConfiguration) -> SignalBinding:
    selector = signal.c37_118_v2_selector
    if selector.kind == "phasor_magnitude":
        channel_name = selector.phasor_magnitude_channel
        if channel_name is None:
            raise NormalizationError("C37.118 phasor selector has no channel name")
        matching_indices = [
            index
            for index, phasor in enumerate(pmu.phasors)
            if phasor.channel_name == channel_name
        ]
        if len(matching_indices) != 1:
            raise NormalizationError(
                f"CFG-2 must contain exactly one configured phasor channel {channel_name!r}"
            )
        phasor = pmu.phasors[matching_indices[0]]
        expected_current = signal.quantity == "current"
        if phasor.is_current != expected_current:
            raise NormalizationError(
                f"CFG-2 phasor channel {channel_name!r} does not match {signal.quantity}"
            )
        return SignalBinding(
            signal=signal,
            selector_kind=selector.kind,
            phasor_index=matching_indices[0],
        )
    if selector.kind in {"frequency", "rocof"}:
        return SignalBinding(signal=signal, selector_kind=selector.kind)
    raise NormalizationError("Source signal has an unsupported C37.118 v2 selector")


def _binding_value(
    binding: SignalBinding,
    phasor_magnitudes: tuple[float, ...],
    frequency_hz: float,
    rocof_hz_per_s: float,
) -> float:
    if binding.selector_kind == "phasor_magnitude":
        if binding.phasor_index is None:
            raise NormalizationError("C37.118 phasor binding has no configured index")
        return phasor_magnitudes[binding.phasor_index]
    if binding.selector_kind == "frequency":
        return frequency_hz
    if binding.selector_kind == "rocof":
        return rocof_hz_per_s
    raise NormalizationError("Source signal has an unsupported C37.118 v2 selector")


def _field_timestamp(soc: int, fracsec: int, time_base: int) -> Timestamp:
    if time_base <= 0 or fracsec < 0 or fracsec >= time_base:
        raise NormalizationError("C37.118 timestamp does not fit its CFG-2 TIME_BASE")
    nanoseconds = (fracsec * 1_000_000_000) // time_base
    return Timestamp(seconds=soc, nanos=nanoseconds)


def _epoch_milliseconds_timestamp(timestamp_ms: int) -> Timestamp:
    seconds, milliseconds = divmod(timestamp_ms, 1_000)
    return Timestamp(seconds=seconds, nanos=milliseconds * 1_000_000)


def _timestamp_nanoseconds(timestamp: Timestamp) -> int:
    return timestamp.seconds * 1_000_000_000 + timestamp.nanos


def _apply_v2_quality(
    measurement: rtd_schema_pb2.MCCSMeasurementValue,
    stat: int,
) -> None:
    data_error = (stat >> 14) & 0b11
    synchronized = not bool(stat & (1 << 13))
    measurement.quality.valid = data_error == 0 and synchronized
    if data_error == 0b10:
        measurement.quality.substituted = True