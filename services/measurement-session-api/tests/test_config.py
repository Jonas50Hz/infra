"""Configuration validation for the session request API."""

from __future__ import annotations

import unittest

from measurement_session_api.config import ConfigurationError, Settings


class SettingsTests(unittest.TestCase):
    """Keep API request bounds compatible with immutable Blobmeta contracts."""

    def test_defaults_match_the_session_processor(self) -> None:
        settings = Settings.from_environment({})

        self.assertEqual(settings.kafka_bootstrap_servers, "kafka:9092")
        self.assertEqual(settings.kafka_topic, "MeasurementSession")
        self.assertEqual(settings.max_interval_hours, 24)
        self.assertEqual(settings.max_mrids, 32)
        self.assertEqual(settings.publish_timeout_seconds, 30)
        self.assertEqual(
            settings.grafana_session_dashboard_url,
            "http://localhost:3001/d/wama-measurement-sessions/wama-measurement-sessions",
        )

    def test_uses_the_configured_grafana_root_for_session_links(self) -> None:
        settings = Settings.from_environment(
            {"GRAFANA_ROOT_URL": "http://192.0.2.10:3001/"}
        )

        self.assertEqual(
            settings.grafana_session_dashboard_url,
            "http://192.0.2.10:3001/d/wama-measurement-sessions/wama-measurement-sessions",
        )

    def test_explicit_dashboard_url_overrides_the_grafana_root(self) -> None:
        settings = Settings.from_environment(
            {
                "GRAFANA_ROOT_URL": "http://192.0.2.10:3001/",
                "GRAFANA_SESSION_DASHBOARD_URL": "https://dashboards.example.test/sessions",
            }
        )

        self.assertEqual(settings.grafana_session_dashboard_url, "https://dashboards.example.test/sessions")

    def test_rejects_mrid_bound_above_contract_ceiling(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "must not exceed 32"):
            Settings.from_environment({"MEASUREMENT_SESSION_MAX_MRIDS": "33"})

    def test_rejects_non_http_dashboard_url(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "absolute HTTP"):
            Settings.from_environment({"GRAFANA_SESSION_DASHBOARD_URL": "/sessions"})


if __name__ == "__main__":
    unittest.main()