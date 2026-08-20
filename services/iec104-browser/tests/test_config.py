"""Configuration validation for the transient IEC 104 browser."""

from __future__ import annotations

import unittest

from iec104_browser.config import ConfigurationError, Settings


class SettingsTests(unittest.TestCase):
    """Require valid target ports and bounded per-page queues."""

    def test_loads_local_defaults(self) -> None:
        settings = Settings.from_environment({})

        self.assertEqual(settings.exporter_host, "iec104-exporter")
        self.assertEqual(settings.exporter_port, 2404)
        self.assertEqual(settings.queue_size, 256)

    def test_rejects_invalid_port_and_queue_size(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "between 1 and 65535"):
            Settings.from_environment({"IEC104_EXPORTER_PORT": "0"})
        with self.assertRaisesRegex(ConfigurationError, "must be greater than zero"):
            Settings.from_environment({"IEC104_BROWSER_QUEUE_SIZE": "0"})


if __name__ == "__main__":
    unittest.main()