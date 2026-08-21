"""Tests for query indexer Kafka reset settings."""

from __future__ import annotations

import unittest

from measurement_session_query_indexer.config import ConfigurationError, Settings


class SettingsTests(unittest.TestCase):
    """Keep the default migration-safe while allowing scoped fresh-record tests."""

    def test_defaults_to_earliest(self) -> None:
        self.assertEqual(Settings.from_environment({}).kafka_auto_offset_reset, "earliest")

    def test_accepts_latest_for_an_isolated_fresh_record_group(self) -> None:
        settings = Settings.from_environment({"KAFKA_AUTO_OFFSET_RESET": "latest"})

        self.assertEqual(settings.kafka_auto_offset_reset, "latest")

    def test_rejects_unknown_offset_reset_policy(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "earliest or latest"):
            Settings.from_environment({"KAFKA_AUTO_OFFSET_RESET": "none"})