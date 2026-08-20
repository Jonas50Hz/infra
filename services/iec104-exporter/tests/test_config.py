"""Settings validation for the IEC 104 exporter."""

from __future__ import annotations

import unittest

from iec104_exporter.config import ConfigurationError, Settings


class SettingsTests(unittest.TestCase):
    """Require a safe listener and dedicated Kafka consumer defaults."""

    def test_loads_defaults(self) -> None:
        settings = Settings.from_environment({})

        self.assertEqual(settings.kafka_topic, "Export")
        self.assertEqual(settings.kafka_consumer_group, "iec104-exporter")
        self.assertEqual(settings.backend_port, 2405)
        self.assertEqual(settings.port, 2404)

    def test_rejects_unsafe_paths_and_ports(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "absolute"):
            Settings.from_environment({"IEC104_READY_FILE": "ready"})
        with self.assertRaisesRegex(ConfigurationError, "between 1 and 65535"):
            Settings.from_environment({"IEC104_PORT": "0"})
        with self.assertRaisesRegex(ConfigurationError, "must differ"):
            Settings.from_environment({"IEC104_PORT": "2404", "IEC104_BACKEND_PORT": "2404"})


if __name__ == "__main__":
    unittest.main()