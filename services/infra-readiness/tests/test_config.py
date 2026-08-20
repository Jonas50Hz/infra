"""Tests for readiness service environment parsing."""

from __future__ import annotations

import unittest

from infra_readiness.config import ConfigurationError, Settings


class SettingsTests(unittest.TestCase):
    """Ensure malformed optional overrides fail before any network call."""

    def test_uses_local_poc_defaults(self) -> None:
        settings = Settings.from_environment({})

        self.assertEqual(settings.druid_router_url, "http://druid:8888")
        self.assertEqual(settings.druid_datasource, "live_measurements")
        self.assertEqual(settings.druid_expected_double_value, 50.01)
        self.assertEqual(settings.druid_expected_double_value_tolerance, 0.01)
        self.assertEqual(settings.iec104_browser_url, "http://iec104-browser:8080")
        self.assertEqual(
            settings.forgejo_processor_repositories,
            (
                "processor-frequency-scale",
                "processor-apparent-power",
                "processor-frequency-iec104-export",
                "processor-lfr-frequency-provision",
            ),
        )
        self.assertEqual(settings.kafka_bootstrap_servers, "kafka:9092")
        self.assertEqual(settings.measurement_session_topic_partitions, 12)
        self.assertEqual(settings.blobmeta_topic_partitions, 12)
        self.assertEqual(settings.s3_buckets, ("wama-raw", "wama-measurement-sessions"))
        self.assertEqual(settings.readiness_timeout_seconds, 180)

    def test_rejects_non_positive_timeout(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "READINESS_TIMEOUT_SECONDS"):
            Settings.from_environment({"READINESS_TIMEOUT_SECONDS": "0"})

    def test_rejects_non_positive_worker_topic_partitions(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "MEASUREMENT_SESSION_TOPIC_PARTITIONS"):
            Settings.from_environment({"MEASUREMENT_SESSION_TOPIC_PARTITIONS": "0"})

    def test_rejects_empty_bucket_configuration(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "S3_BUCKETS"):
            Settings.from_environment({"S3_BUCKETS": " , "})

    def test_rejects_repeated_processor_repositories(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "must not repeat"):
            Settings.from_environment(
                {
                    "FORGEJO_PROCESSOR_REPOSITORIES": (
                        "processor-frequency-scale,processor-frequency-scale"
                    )
                }
            )

    def test_rejects_non_http_service_url(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "FORGEJO_URL"):
            Settings.from_environment({"FORGEJO_URL": "forgejo:3000"})
        with self.assertRaisesRegex(ConfigurationError, "IEC104_BROWSER_URL"):
            Settings.from_environment({"IEC104_BROWSER_URL": "iec104-browser:8080"})

    def test_rejects_non_finite_druid_expected_value(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "DRUID_EXPECTED_DOUBLE_VALUE"):
            Settings.from_environment({"DRUID_EXPECTED_DOUBLE_VALUE": "nan"})

    def test_rejects_negative_or_non_finite_druid_expected_value_tolerance(self) -> None:
        for tolerance in ("-0.01", "nan"):
            with self.subTest(tolerance=tolerance):
                with self.assertRaisesRegex(
                    ConfigurationError,
                    "DRUID_EXPECTED_DOUBLE_VALUE_TOLERANCE",
                ):
                    Settings.from_environment(
                        {"DRUID_EXPECTED_DOUBLE_VALUE_TOLERANCE": tolerance}
                    )