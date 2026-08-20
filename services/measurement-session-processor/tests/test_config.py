"""Tests for MeasurementSession worker environment bounds."""

from __future__ import annotations

import unittest

from measurement_session_processor.config import ConfigurationError, Settings


class SettingsTests(unittest.TestCase):
    """Ensure resource limits fail before network clients start."""

    def test_uses_bounded_defaults(self) -> None:
        settings = Settings.from_environment({})

        self.assertEqual(settings.kafka_topic, "MeasurementSession")
        self.assertEqual(settings.blobmeta_topic, "Blobmeta")
        self.assertEqual(settings.max_mrids, 32)
        self.assertEqual(settings.max_interval_hours, 24)

    def test_rejects_non_positive_row_limit(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "MEASUREMENT_SESSION_MAX_ROWS"):
            Settings.from_environment({"MEASUREMENT_SESSION_MAX_ROWS": "0"})

    def test_rejects_mrid_limit_above_blobmeta_contract(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "MEASUREMENT_SESSION_MAX_MRIDS"):
            Settings.from_environment({"MEASUREMENT_SESSION_MAX_MRIDS": "33"})

    def test_rejects_noncanonical_session_bucket(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "S3_BUCKET"):
            Settings.from_environment({"S3_BUCKET": "unexpected"})