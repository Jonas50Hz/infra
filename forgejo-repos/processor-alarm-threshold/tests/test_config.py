"""Tests for strict reviewed alarm threshold configuration."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from processor_alarm_threshold.config import ConfigurationError, Settings, load_reviewed_rules


class ConfigurationTests(unittest.TestCase):
    """Reject configuration that changes reviewed alarm semantics implicitly."""

    def test_loads_initial_frequency_rule_and_matches_whole_mrid_segments(self) -> None:
        configuration = load_reviewed_rules(self._initial_configuration_path())

        self.assertEqual(configuration.catalog_id, "c37-118-poc-v1")
        self.assertEqual(len(configuration.rules), 1)
        rule = configuration.rules[0]
        self.assertEqual(rule.rule_id, "frequency-over-50-1-hz")
        self.assertEqual(rule.threshold, 50.1)
        self.assertTrue(rule.selector.matches_mrid("urn:wama:poc:pmu:bay-01:frequency"))
        self.assertFalse(rule.selector.matches_mrid("urn:wama:poc:pmu:bay-01:rocof"))
        self.assertFalse(rule.selector.matches_mrid("urn:wama:poc:pmu:bay-01:phase:frequency"))

    def test_rejects_unknown_rule_fields_and_non_frequency_semantics(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "unsupported field"):
            self._load(
                """\
version: 1
catalog_id: c37-118-poc-v1
rules:
  - rule_id: frequency-over-50-1-hz
    revision: "1"
    selector:
      mrid_glob: urn:wama:poc:pmu:*:frequency
      value_kind: double
      quantity: frequency
      unit: Hz
    operator: gt
    threshold: 50.1
    severity: warning
    notify: true
"""
            )
        with self.assertRaisesRegex(ConfigurationError, "quantity must be 'frequency'"):
            self._load(
                """\
version: 1
catalog_id: c37-118-poc-v1
rules:
  - rule_id: frequency-over-50-1-hz
    revision: "1"
    selector:
      mrid_glob: urn:wama:poc:pmu:*:frequency
      value_kind: double
      quantity: voltage
      unit: Hz
    operator: gt
    threshold: 50.1
    severity: warning
"""
            )

    def test_rejects_unanchored_or_partial_segment_globs(self) -> None:
        for mrid_glob in ("*:frequency", "urn:wama:poc:pmu:bay-*:frequency", "urn::frequency"):
            with self.subTest(mrid_glob=mrid_glob):
                with self.assertRaisesRegex(ConfigurationError, "mrid_glob"):
                    self._load(
                        f"""\
version: 1
catalog_id: c37-118-poc-v1
rules:
  - rule_id: frequency-over-50-1-hz
    revision: "1"
    selector:
      mrid_glob: "{mrid_glob}"
      value_kind: double
      quantity: frequency
      unit: Hz
    operator: gt
    threshold: 50.1
    severity: warning
"""
                    )

    def test_settings_require_an_absolute_configuration_path(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "absolute path"):
            Settings.from_environment({"ALARM_THRESHOLD_CONFIG_PATH": "config.yaml"})

    def test_settings_default_watermark_topic(self) -> None:
        settings = Settings.from_environment(
            {"ALARM_THRESHOLD_CONFIG_PATH": str(self._initial_configuration_path())}
        )

        self.assertEqual(settings.alarm_evaluation_watermark_topic, "AlarmEvaluationWatermark")

    def test_migration_fence_requires_the_exact_reviewed_token(self) -> None:
        environment = {
            "ALARM_THRESHOLD_CONFIG_PATH": str(self._initial_configuration_path()),
            "WAMA_ALARM_EVALUATION_WATERMARK_MIGRATION": (
                "accept-forward-only-alarm-evaluation-watermark-v1"
            ),
        }

        self.assertTrue(Settings.from_environment(environment).allow_legacy_active_alarm_bootstrap)
        for token in (
            "accept-forward-only-alarm-evaluation-watermark-v2",
            "ACCEPT-FORWARD-ONLY-ALARM-EVALUATION-WATERMARK-V1",
        ):
            with self.subTest(token=token):
                with self.assertRaisesRegex(ConfigurationError, "must be"):
                    Settings.from_environment(
                        {**environment, "WAMA_ALARM_EVALUATION_WATERMARK_MIGRATION": token}
                    )

    def _load(self, contents: str) -> object:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "alarm-threshold.yaml"
            path.write_text(contents, encoding="utf-8")
            return load_reviewed_rules(path)

    @staticmethod
    def _initial_configuration_path() -> Path:
        installed_path = Path("/etc/wama/alarm-threshold.yaml")
        if installed_path.is_file():
            return installed_path
        return Path(__file__).resolve().parents[1] / "config" / "alarm-threshold.yaml"


if __name__ == "__main__":
    unittest.main()