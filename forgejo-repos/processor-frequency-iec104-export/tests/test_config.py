"""Settings tests for the direct frequency IEC 104 export processor."""

from __future__ import annotations

import unittest

from processor_frequency_iec104_export.config import ConfigurationError, Settings


class SettingsTests(unittest.TestCase):
    """Keep the direct PoC mapping explicit and bounded."""

    def test_uses_documented_direct_export_defaults(self) -> None:
        settings = Settings.from_environment({})

        self.assertEqual(settings.source_mrid, "urn:wama:poc:pmu:bay-01:frequency")
        self.assertEqual(settings.common_address, 1)
        self.assertEqual(settings.information_object_address, 1001)
        self.assertEqual(settings.cause_code, 3)
        self.assertEqual(settings.input_topic, "LiveMeasurement")
        self.assertEqual(settings.output_topic, "Export")

    def test_rejects_out_of_range_or_control_direction_mapping(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "INFORMATION_OBJECT_ADDRESS"):
            Settings.from_environment({"FREQUENCY_IEC104_INFORMATION_OBJECT_ADDRESS": "0"})
        with self.assertRaisesRegex(ConfigurationError, "monitor-direction"):
            Settings.from_environment({"FREQUENCY_IEC104_CAUSE_CODE": "6"})
        with self.assertRaisesRegex(ConfigurationError, "must not be empty"):
            Settings.from_environment({"FREQUENCY_IEC104_SOURCE_MRID": " "})


if __name__ == "__main__":
    unittest.main()