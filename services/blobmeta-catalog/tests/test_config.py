"""Tests for Blobmeta catalog settings."""

from __future__ import annotations

import unittest

from blobmeta_catalog.config import ConfigurationError, Settings


class SettingsTests(unittest.TestCase):
    """Ensure retries are bounded before the worker contacts Kafka."""

    def test_uses_blobmeta_defaults(self) -> None:
        settings = Settings.from_environment({})

        self.assertEqual(settings.kafka_topic, "Blobmeta")
        self.assertEqual(settings.kafka_consumer_group, "blobmeta-catalog")

    def test_rejects_non_positive_retry_interval(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "KAFKA_RETRY_INTERVAL_SECONDS"):
            Settings.from_environment({"KAFKA_RETRY_INTERVAL_SECONDS": "0"})