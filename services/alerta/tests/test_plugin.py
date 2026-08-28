"""Tests for the Alerta post-receive-only WAMA Mailpit plugin."""

from __future__ import annotations

from types import SimpleNamespace
import unittest
from unittest.mock import patch

from flask import Flask
from importlib.metadata import entry_points

from wama_alerta_mailer.plugin import (
    WAMA_MANAGED_BY_ATTRIBUTE,
    WAMA_MANAGED_BY_VALUE,
    WAMA_MANAGED_TAG,
    WamaInitialEpisodeMailer,
)


class WamaInitialEpisodeMailerTests(unittest.TestCase):
    """Require every first-episode predicate before sending local SMTP mail."""

    def setUp(self) -> None:
        self._app = Flask(__name__)
        self._context = self._app.app_context()
        self._context.push()
        self._plugin = WamaInitialEpisodeMailer()
        self._config = {"WAMA_ALERT_RECIPIENT": "wama-alerts@local.test"}

    def tearDown(self) -> None:
        self._context.pop()

    def test_sends_once_for_exactly_one_new_wama_episode(self) -> None:
        with patch("wama_alerta_mailer.plugin.Mailer") as mailer:
            result = self._plugin.post_receive(_alert(), config=self._config)

        self.assertEqual(result.resource, "urn:wama:poc:pmu:bay-01:frequency")
        mailer.return_value.send_email.assert_called_once()
        recipient, subject, body = mailer.return_value.send_email.call_args.args
        self.assertEqual(recipient, "wama-alerts@local.test")
        self.assertIn("urn:wama:poc:pmu:bay-01:frequency", subject)
        self.assertIn("new WAMA alarm episode", body)

    def test_does_not_send_for_non_initial_or_unmanaged_alerts(self) -> None:
        for change in (
            {"repeat": True},
            {"duplicate_count": 1},
            {"previous_severity": "warning"},
            {"status": "ack"},
            {"history": [SimpleNamespace(type="new"), SimpleNamespace(type="ack")]},
            {"tags": []},
            {"attributes": {}},
        ):
            with self.subTest(change=change), patch("wama_alerta_mailer.plugin.Mailer") as mailer:
                self._plugin.post_receive(_alert(**change), config=self._config)

            mailer.return_value.send_email.assert_not_called()

    def test_entry_point_is_discoverable_by_alerta(self) -> None:
        plugins = entry_points(group="alerta.plugins")

        self.assertIn("wama_initial_episode_mailer", {plugin.name for plugin in plugins})


def _alert(**changes: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "attributes": {WAMA_MANAGED_BY_ATTRIBUTE: WAMA_MANAGED_BY_VALUE},
        "duplicate_count": 0,
        "event": "wama-alarm/a0d5e631-962d-4ce3-86ba-04d4252a3285",
        "history": [SimpleNamespace(type="new")],
        "previous_severity": "indeterminate",
        "repeat": False,
        "resource": "urn:wama:poc:pmu:bay-01:frequency",
        "severity": "indeterminate",
        "status": "open",
        "tags": [WAMA_MANAGED_TAG],
        "text": "[WAMA WARNING] Frequency exceeds threshold",
    }
    values.update(changes)
    return SimpleNamespace(**values)