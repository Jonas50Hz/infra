"""Tests for ingress configuration validation."""

from __future__ import annotations

import unittest

from alarm_alerta_ingress.config import ConfigurationError, Settings


class SettingsTests(unittest.TestCase):
    """Reject unsafe endpoint and timing settings before touching Kafka or Alerta."""

    def test_uses_root_owned_defaults(self) -> None:
        settings = Settings.from_environment({})

        self.assertEqual(settings.alerta_api_key, "wama-alerta-ingress-local-api-key-0001")
        self.assertEqual(settings.alerta_url, "http://alerta:8080")
        self.assertEqual(settings.kafka_topic, "Alarm")

    def test_rejects_non_http_alerta_url(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "ALERTA_URL"):
            Settings.from_environment({"ALERTA_URL": "alerta:8080"})

    def test_rejects_non_positive_retry_interval(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "KAFKA_RETRY_INTERVAL_SECONDS"):
            Settings.from_environment({"KAFKA_RETRY_INTERVAL_SECONDS": "0"})