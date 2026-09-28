"""Tests for C37.118 v2 to Common Format normalization."""

from __future__ import annotations

import unittest

from gateway_c37_118_onboarding.c37_118_v2 import (
    ConfigurationFrame,
    DataFrame,
    FrameHeader,
    PhasorConfiguration,
    PmuConfiguration,
    PmuData,
)
from gateway_c37_118_onboarding.config import (
    C37_118V2SignalSelectorDefinition,
    SignalDefinition,
    SourceDefinition,
)
from gateway_c37_118_onboarding.normalization import (
    NormalizationError,
    build_source_mapping,
    normalize_data_frame,
)


class NormalizationTests(unittest.TestCase):
    """Keep source semantics, timestamps, and quality mapping explicit."""

    def test_normalizes_all_supported_catalog_signal_kinds(self) -> None:
        mapping = build_source_mapping(_source(), _configuration())

        measurements = normalize_data_frame(mapping, _data_frame(), 1_700_000_000_600)
        measurements_by_mrid = {measurement.mrid: measurement for measurement in measurements}

        self.assertEqual(len(measurements), 4)
        self.assertEqual(
            measurements_by_mrid["urn:wama:test:voltage"].double_value,
            230_000.0,
        )
        self.assertEqual(
            measurements_by_mrid["urn:wama:test:current"].double_value,
            500.0,
        )
        self.assertAlmostEqual(
            measurements_by_mrid["urn:wama:test:frequency"].double_value,
            50.01,
        )
        self.assertAlmostEqual(
            measurements_by_mrid["urn:wama:test:rocof"].double_value,
            -0.02,
        )
        frequency = measurements_by_mrid["urn:wama:test:frequency"]
        self.assertEqual(frequency.timestamp_field.seconds, 1_700_000_000)
        self.assertEqual(frequency.timestamp_field.nanos, 500_000_000)
        self.assertEqual(frequency.timestamp_gateway.nanos, 600_000_000)
        self.assertEqual(frequency.timestamp_mccs.nanos, 600_000_000)
        self.assertTrue(frequency.quality.valid)
        self.assertFalse(frequency.quality.HasField("substituted"))

    def test_marks_unsynchronized_test_data_invalid_and_substituted(self) -> None:
        mapping = build_source_mapping(_source(), _configuration())
        data_frame = _data_frame(stat=0b1010_0000_0000_0000)

        measurements = normalize_data_frame(mapping, data_frame, 1_700_000_000_600)

        for measurement in measurements:
            self.assertFalse(measurement.quality.valid)
            self.assertTrue(measurement.quality.substituted)

    def test_rejects_a_future_field_timestamp(self) -> None:
        mapping = build_source_mapping(_source(), _configuration())

        with self.assertRaisesRegex(NormalizationError, "later than gateway"):
            normalize_data_frame(mapping, _data_frame(), 1_700_000_000_499)

    def test_rejects_a_phasor_type_mismatch(self) -> None:
        voltage_signal = _source().signals[0]
        incorrect_signal = SignalDefinition(
            signal_id="current-but-voltage",
            source_channel="VL1",
            mrid="urn:wama:test:incorrect",
            value_kind="double",
            quantity="current",
            unit="A",
            c37_118_v2_selector=voltage_signal.c37_118_v2_selector,
        )
        source = SourceDefinition(
            source_id="pmu-test",
            site_id="wama-test",
            display_name="WAMA Test PMU",
            ip_address="192.0.2.10",
            port=4712,
            pmu_idcode=1001,
            wire_version=2,
            signals=(incorrect_signal,),
        )

        with self.assertRaisesRegex(NormalizationError, "does not match current"):
            build_source_mapping(source, _configuration())


def _source() -> SourceDefinition:
    return SourceDefinition(
        source_id="pmu-test",
        site_id="wama-test",
        display_name="WAMA Test PMU",
        ip_address="192.0.2.10",
        port=4712,
        pmu_idcode=1001,
        wire_version=2,
        signals=(
            _signal("voltage", "VL1", "voltage", "V", "phasor_magnitude", "VL1"),
            _signal("current", "IL1", "current", "A", "phasor_magnitude", "IL1"),
            _signal("frequency", "FREQ", "frequency", "Hz", "frequency"),
            _signal("rocof", "ROCOF", "rocof", "Hz/s", "rocof"),
        ),
    )


def _signal(
    signal_id: str,
    source_channel: str,
    quantity: str,
    unit: str,
    selector_kind: str,
    phasor_magnitude_channel: str | None = None,
) -> SignalDefinition:
    return SignalDefinition(
        signal_id=signal_id,
        source_channel=source_channel,
        mrid=f"urn:wama:test:{signal_id}",
        value_kind="double",
        quantity=quantity,
        unit=unit,
        c37_118_v2_selector=C37_118V2SignalSelectorDefinition(
            kind=selector_kind,
            phasor_magnitude_channel=phasor_magnitude_channel,
        ),
    )


def _configuration() -> ConfigurationFrame:
    return ConfigurationFrame(
        header=FrameHeader(
            frame_type=3,
            idcode=1001,
            soc=1_700_000_000,
            message_time_quality=0,
            fracsec=0,
        ),
        time_base=1_000_000,
        pmus=(
            PmuConfiguration(
                idcode=1001,
                station_name="PMU-ONE",
                phasors=(
                    PhasorConfiguration("VL1", False, 1.0),
                    PhasorConfiguration("IL1", True, 1.0),
                ),
                analog_count=0,
                digital_word_count=0,
                phasor_is_float=True,
                phasor_is_polar=True,
                frequency_is_float=True,
                analog_is_float=True,
                nominal_frequency_hz=50,
                configuration_count=3,
            ),
        ),
        data_rate=50,
    )


def _data_frame(stat: int = 0) -> DataFrame:
    return DataFrame(
        header=FrameHeader(
            frame_type=0,
            idcode=1001,
            soc=1_700_000_000,
            message_time_quality=0,
            fracsec=500_000,
        ),
        pmus=(
            PmuData(
                idcode=1001,
                stat=stat,
                phasor_magnitudes=(230_000.0, 500.0),
                frequency_hz=50.01,
                rocof_hz_per_s=-0.02,
            ),
        ),
    )