"""Configuration tests for the direct frequency IEC 104 export processor."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from processor_frequency_iec104_export.config import (
    ConfigurationError,
    Settings,
    load_export_mappings,
)


class SettingsTests(unittest.TestCase):
    """Keep reviewed direct-PoC mappings explicit and bounded."""

    def test_loads_the_packaged_five_gateway_map(self) -> None:
        settings = Settings.from_environment({})

        self.assertEqual(settings.config_path, "/etc/wama/frequency-iec104-export.yaml")
        self.assertEqual(len(settings.mappings), 5)
        for bay in range(1, 6):
            mapping = settings.mapping_for(f"urn:wama:poc:pmu:bay-{bay:02}:frequency")
            self.assertIsNotNone(mapping)
            assert mapping is not None
            self.assertEqual(mapping.common_address, 1000 + bay)
            self.assertEqual(mapping.information_object_address, 1001)
            self.assertEqual(mapping.cause_code, 3)
        self.assertEqual(settings.input_topic, "LiveMeasurement")
        self.assertEqual(settings.output_topic, "Export")

    def test_rejects_legacy_single_mapping_settings(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "Legacy single-mapping"):
            Settings.from_environment({"FREQUENCY_IEC104_SOURCE_MRID": "urn:wama:test:frequency"})

    def test_rejects_duplicate_or_control_direction_mappings(self) -> None:
        duplicate_point = self._valid_configuration().replace(
            "common_address: 1002",
            "common_address: 1001",
        )
        with self.assertRaisesRegex(ConfigurationError, "duplicates IEC point"):
            self._load(duplicate_point)

        unsupported_cause = self._valid_configuration().replace("cause_code: 3", "cause_code: 6")
        with self.assertRaisesRegex(ConfigurationError, "monitor-direction"):
            self._load(unsupported_cause)

    def test_rejects_unknown_or_incomplete_configuration(self) -> None:
        unknown_field = self._valid_configuration().replace("version: 1", "version: 1\nunsupported: true")
        with self.assertRaisesRegex(ConfigurationError, "unsupported field"):
            self._load(unknown_field)

        with self.assertRaisesRegex(ConfigurationError, "non-empty list"):
            self._load("version: 1\nexports: []\n")

    def _load(self, content: str):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "frequency-iec104-export.yaml"
            path.write_text(content, encoding="utf-8")
            return load_export_mappings(path)

    def _valid_configuration(self) -> str:
        return """\
version: 1
exports:
  - mrid: urn:wama:test:pmu-a:frequency
    common_address: 1001
    information_object_address: 1001
    cause_code: 3
  - mrid: urn:wama:test:pmu-b:frequency
    common_address: 1002
    information_object_address: 1001
    cause_code: 3
"""


if __name__ == "__main__":
    unittest.main()