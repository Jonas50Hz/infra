"""Tests for readiness service environment parsing."""

from __future__ import annotations

import unittest

from infra_readiness.config import ConfigurationError, Settings


class SettingsTests(unittest.TestCase):
    """Ensure malformed optional overrides fail before any network call."""

    def test_uses_local_poc_defaults(self) -> None:
        settings = Settings.from_environment({})

        self.assertEqual(settings.kafka_bootstrap_servers, "kafka:9092")
        self.assertEqual(settings.s3_buckets, ("wama-raw", "wama-measurement-sessions"))
        self.assertEqual(settings.readiness_timeout_seconds, 180)

    def test_rejects_non_positive_timeout(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "READINESS_TIMEOUT_SECONDS"):
            Settings.from_environment({"READINESS_TIMEOUT_SECONDS": "0"})

    def test_rejects_empty_bucket_configuration(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "S3_BUCKETS"):
            Settings.from_environment({"S3_BUCKETS": " , "})

    def test_rejects_non_http_service_url(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "FORGEJO_URL"):
            Settings.from_environment({"FORGEJO_URL": "forgejo:3000"})