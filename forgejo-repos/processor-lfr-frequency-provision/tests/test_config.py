"""Tests for LFR deployment configuration validation."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from processor_lfr_frequency_provision.config import ConfigurationError, load_config
from processor_lfr_frequency_provision.selection import EvenMedianTieBreak


class LfrConfigurationTests(unittest.TestCase):
    """Require explicit engineering inputs before the LFR process can start."""

    def test_loads_a_complete_multiple_pmu_configuration(self) -> None:
        config = self._load(self._valid_configuration())

        self.assertEqual(config.close_delay_ms, 600)
        self.assertEqual(config.even_median_tie_break, EvenMedianTieBreak.LOWER_FREQUENCY)
        self.assertEqual(config.signal_for("urn:wama:test:pmu-a:frequency").quantity, "frequency")
        self.assertEqual(config.signal_for("urn:wama:test:pmu-b:voltage").pmu.pmu_id, "pmu-b")
        self.assertIsNone(config.signal_for("urn:wama:test:other"))

    def test_rejects_unresolved_status_evidence_mode(self) -> None:
        content = self._valid_configuration().replace(
            "mode: generic_quality_provisional",
            "mode: quality_valid_only",
        )

        with self.assertRaisesRegex(ConfigurationError, "status_evidence.mode"):
            self._load(content)

    def test_rejects_an_incomplete_count_boundary_policy(self) -> None:
        content = self._valid_configuration().replace(
            "very_good_minimum_good_samples: 26",
            "very_good_minimum_good_samples: 9",
            1,
        )

        with self.assertRaisesRegex(ConfigurationError, "must exceed"):
            self._load(content)

    def test_rejects_a_feedback_output_mrid(self) -> None:
        content = self._valid_configuration().replace(
            "output_mrid: urn:wama:test:lfr:preferred-frequency",
            "output_mrid: urn:wama:test:pmu-a:frequency",
        )

        with self.assertRaisesRegex(ConfigurationError, "must not also be an input"):
            self._load(content)

    def _load(self, content: str):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "lfr-config.yaml"
            path.write_text(content, encoding="utf-8")
            return load_config(path)

    def _valid_configuration(self) -> str:
        return """\
close_delay_ms: 600
even_median_tie_break: lower_frequency
frequency_band_hz:
  minimum: 49.0
  maximum: 51.0
maximum_future_seconds: 1
output_mrid: urn:wama:test:lfr:preferred-frequency
status_evidence:
  mode: generic_quality_provisional
pmus:
  - id: pmu-a
    frequency_mrid: urn:wama:test:pmu-a:frequency
    voltage_mrid: urn:wama:test:pmu-a:voltage
    nominal_voltage: 230.0
    count_thresholds:
      bad_maximum_good_samples: 9
      very_good_minimum_good_samples: 26
    voltage_thresholds:
      very_good_maximum_deviation: 1.0
      good_maximum_deviation: 2.0
  - id: pmu-b
    frequency_mrid: urn:wama:test:pmu-b:frequency
    voltage_mrid: urn:wama:test:pmu-b:voltage
    nominal_voltage: 400.0
    count_thresholds:
      bad_maximum_good_samples: 9
      very_good_minimum_good_samples: 26
    voltage_thresholds:
      very_good_maximum_deviation: 2.0
      good_maximum_deviation: 4.0
"""


if __name__ == "__main__":
    unittest.main()