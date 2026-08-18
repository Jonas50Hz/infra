"""Tests for Druid supervisor initializer environment parsing."""

from __future__ import annotations

import unittest

from druid_init.config import ConfigurationError, Settings


class SettingsTests(unittest.TestCase):
    """Ensure invalid initialization settings fail before network activity."""

    def test_uses_local_poc_defaults(self) -> None:
        settings = Settings.from_environment({})

        self.assertEqual(settings.router_url, "http://druid:8888")
        self.assertEqual(settings.supervisor_id, "live_measurements")
        self.assertEqual(settings.timeout_seconds, 240)

    def test_rejects_non_positive_timeout(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "DRUID_INIT_TIMEOUT_SECONDS"):
            Settings.from_environment({"DRUID_INIT_TIMEOUT_SECONDS": "0"})

    def test_rejects_relative_supervisor_path(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "DRUID_SUPERVISOR_SPEC_PATH"):
            Settings.from_environment({"DRUID_SUPERVISOR_SPEC_PATH": "supervisor.json"})

    def test_rejects_non_http_router_url(self) -> None:
        with self.assertRaisesRegex(ConfigurationError, "DRUID_ROUTER_URL"):
            Settings.from_environment({"DRUID_ROUTER_URL": "druid:8888"})