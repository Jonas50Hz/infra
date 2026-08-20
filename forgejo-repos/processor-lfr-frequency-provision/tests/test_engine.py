"""Tests for stateful per-second LFR evaluation."""

from __future__ import annotations

import unittest

from processor_lfr_frequency_provision.classification import CountThresholds, VoltageThresholds
from processor_lfr_frequency_provision.config import LfrConfig, PmuConfig
from processor_lfr_frequency_provision.engine import (
    IncomingMeasurement,
    LfrSecondEngine,
    QualityEvidence,
    RejectionReason,
)
from processor_lfr_frequency_provision.selection import EvenMedianTieBreak, PmuQuality


class LfrSecondEngineTests(unittest.TestCase):
    """Require final, bounded, and explainable LFR seconds."""

    def test_closes_a_second_once_and_selects_the_best_pmu_class(self) -> None:
        engine = LfrSecondEngine(self._config())
        second_ms = 1_726_000_123_000

        self.assertTrue(engine.ingest(self._frequency("pmu-a", 49.99, second_ms, "a-f"), second_ms).accepted)
        self.assertTrue(engine.ingest(self._voltage("pmu-a", 230.2, second_ms, "a-v"), second_ms).accepted)
        self.assertTrue(engine.ingest(self._frequency("pmu-b", 50.03, second_ms, "b-f"), second_ms).accepted)
        self.assertTrue(engine.ingest(self._voltage("pmu-b", 401.5, second_ms, "b-v"), second_ms).accepted)

        closed = engine.close_ready(second_ms + 1_600)

        self.assertEqual(len(closed), 1)
        result = closed[0]
        self.assertEqual(result.second, second_ms // 1_000)
        self.assertIsNotNone(result.preferred_frequency)
        assert result.preferred_frequency is not None
        self.assertEqual(result.preferred_frequency.frequency_hz, 49.99)
        self.assertEqual(result.preferred_frequency.quality, PmuQuality.VERY_GOOD)
        self.assertEqual(result.preferred_frequency.selected_pmu_ids, ("pmu-a",))
        self.assertEqual(engine.close_ready(second_ms + 1_700), ())

    def test_retains_all_plausibility_failures_for_a_frequency_sample(self) -> None:
        engine = LfrSecondEngine(self._config())
        timestamp_ms = 1_726_000_123_000

        result = engine.ingest(
            IncomingMeasurement(
                input_id="invalid",
                mrid="urn:wama:test:pmu-a:frequency",
                double_value=52.0,
                quality=QualityEvidence(valid=False, overflow=True),
                timestamp_field_ms=timestamp_ms,
            ),
            timestamp_ms,
        )

        self.assertFalse(result.accepted)
        self.assertEqual(
            result.reasons,
            (
                RejectionReason.INVALID_STATUS,
                RejectionReason.OVERFLOW,
                RejectionReason.OUT_OF_BAND,
            ),
        )
        closed = engine.close_ready(timestamp_ms + 1_600)
        self.assertEqual(closed[0].rejection_counts[RejectionReason.INVALID_STATUS.value], 1)
        self.assertEqual(closed[0].rejection_counts[RejectionReason.OVERFLOW.value], 1)
        self.assertEqual(closed[0].rejection_counts[RejectionReason.OUT_OF_BAND.value], 1)

    def test_rejects_missing_future_and_late_source_timestamps(self) -> None:
        engine = LfrSecondEngine(self._config())
        timestamp_ms = 1_726_000_123_000
        missing = engine.ingest(
            IncomingMeasurement(
                input_id="missing",
                mrid="urn:wama:test:pmu-a:frequency",
                double_value=50.0,
                quality=QualityEvidence(valid=True),
                timestamp_field_ms=None,
            ),
            timestamp_ms,
        )
        future = engine.ingest(
            self._frequency("pmu-a", 50.0, timestamp_ms + 2_000, "future"),
            timestamp_ms,
        )
        engine.close_ready(timestamp_ms + 1_600)
        late = engine.ingest(self._frequency("pmu-a", 50.0, timestamp_ms, "late"), timestamp_ms + 1_700)

        self.assertEqual(missing.reasons, (RejectionReason.MISSING_TIMESTAMP,))
        self.assertEqual(future.reasons, (RejectionReason.FUTURE_TIMESTAMP,))
        self.assertEqual(late.reasons, (RejectionReason.LATE_AFTER_CLOSE,))

    def test_deduplicates_replayed_inputs_and_restores_open_state(self) -> None:
        timestamp_ms = 1_726_000_123_000
        engine = LfrSecondEngine(self._config())
        source = self._frequency("pmu-a", 50.0, timestamp_ms, "same-offset")

        self.assertTrue(engine.ingest(source, timestamp_ms).accepted)
        self.assertFalse(engine.ingest(source, timestamp_ms).accepted)
        restored = LfrSecondEngine(self._config(), engine.snapshot())
        self.assertFalse(restored.ingest(source, timestamp_ms).accepted)
        self.assertTrue(restored.ingest(self._voltage("pmu-a", 230.0, timestamp_ms, "voltage"), timestamp_ms).accepted)

        closed = restored.close_ready(timestamp_ms + 1_600)
        self.assertEqual(closed[0].pmus[0].good_frequency_count, 1)
        self.assertEqual(closed[0].pmus[0].total_frequency_count, 1)

    def _config(self) -> LfrConfig:
        thresholds = CountThresholds(0, 1)
        voltage_thresholds = VoltageThresholds(1.0, 2.0)
        return LfrConfig(
            close_delay_ms=600,
            even_median_tie_break=EvenMedianTieBreak.LOWER_FREQUENCY,
            frequency_min_hz=49.0,
            frequency_max_hz=51.0,
            maximum_future_seconds=1,
            output_mrid="urn:wama:test:lfr:preferred-frequency",
            pmus=(
                PmuConfig(
                    pmu_id="pmu-a",
                    frequency_mrid="urn:wama:test:pmu-a:frequency",
                    voltage_mrid="urn:wama:test:pmu-a:voltage",
                    nominal_voltage=230.0,
                    count_thresholds=thresholds,
                    voltage_thresholds=voltage_thresholds,
                ),
                PmuConfig(
                    pmu_id="pmu-b",
                    frequency_mrid="urn:wama:test:pmu-b:frequency",
                    voltage_mrid="urn:wama:test:pmu-b:voltage",
                    nominal_voltage=400.0,
                    count_thresholds=thresholds,
                    voltage_thresholds=voltage_thresholds,
                ),
            ),
            status_evidence_mode="generic_quality_provisional",
        )

    def _frequency(
        self,
        pmu_id: str,
        value: float,
        timestamp_ms: int,
        input_id: str,
    ) -> IncomingMeasurement:
        return IncomingMeasurement(
            input_id=input_id,
            mrid=f"urn:wama:test:{pmu_id}:frequency",
            double_value=value,
            quality=QualityEvidence(valid=True),
            timestamp_field_ms=timestamp_ms,
        )

    def _voltage(
        self,
        pmu_id: str,
        value: float,
        timestamp_ms: int,
        input_id: str,
    ) -> IncomingMeasurement:
        return IncomingMeasurement(
            input_id=input_id,
            mrid=f"urn:wama:test:{pmu_id}:voltage",
            double_value=value,
            quality=QualityEvidence(valid=True),
            timestamp_field_ms=timestamp_ms,
        )


if __name__ == "__main__":
    unittest.main()