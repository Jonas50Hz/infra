"""Tests for fake PMU gateway startup configuration validation."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from pmu_gateway.config import ConfigurationError, load_config


class GatewayConfigTests(unittest.TestCase):
    """Validate the external YAML contract before Kafka is involved."""

    def test_loads_typed_values_and_environment_interval_override(self) -> None:
        config = self._load(
            """
publish_interval_ms: 500
messages:
  - mrid: urn:wama:poc:test:frequency
    value:
      double_value: 50.01
    quality:
      valid: true
    field_timestamp_offset_ms: 20
  - mrid: urn:wama:poc:test:state
    value:
      uint_value: 2
""",
            interval_override="250",
        )

        self.assertEqual(config.publish_interval_ms, 250)
        self.assertEqual(len(config.messages), 2)
        self.assertEqual(config.messages[0].value_field, "double_value")
        self.assertEqual(config.messages[0].quality, {"valid": True})
        self.assertEqual(config.messages[1].value, 2)

    def test_rejects_multiple_oneof_values(self) -> None:
        with self.assertRaisesRegex(
            ConfigurationError,
            "exactly one Common Format value field",
        ):
            self._load(
                """
messages:
  - mrid: urn:wama:poc:test:invalid
    value:
      double_value: 1.0
      bool_value: true
"""
            )

    def test_rejects_unknown_quality_flag(self) -> None:
        with self.assertRaisesRegex(
            ConfigurationError,
            "unsupported key\\(s\\): approximate",
        ):
            self._load(
                """
messages:
  - mrid: urn:wama:poc:test:invalid-quality
    value:
      bool_value: true
    quality:
      approximate: true
"""
            )

    def _load(self, contents: str, interval_override: str | None = None):
        with TemporaryDirectory() as directory:
            config_path = Path(directory) / "messages.yaml"
            config_path.write_text(contents, encoding="utf-8")
            return load_config(config_path, interval_override)


if __name__ == "__main__":
    unittest.main()