"""Configuration validation for the CSV export service."""

from __future__ import annotations

import unittest

from measurement_session_exporter.config import ConfigurationError, Settings


class SettingsTests(unittest.TestCase):
    """Keep the exporter on the trusted read-only query boundary."""

    def test_defaults_use_the_public_read_only_trino_coordinator(self) -> None:
        settings = Settings.from_environment({})

        self.assertEqual(settings.trino_url, "http://trino:8080")
        self.assertEqual(settings.trino_user, "measurement-session-exporter")

    def test_rejects_non_http_trino_url(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "absolute HTTP"):
            Settings.from_environment({"TRINO_URL": "postgres://postgres"})

    def test_rejects_empty_trino_user(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "must not be empty"):
            Settings.from_environment({"TRINO_USER": " "})


if __name__ == "__main__":
    unittest.main()