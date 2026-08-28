"""Tests for active desired-state folding."""

from __future__ import annotations

import unittest

from alarm_alerta_ingress.codec import decode_alarm
from alarm_alerta_ingress.state import AlarmRegistry
from test_codec import _alarm_record


class AlarmRegistryTests(unittest.TestCase):
    """Keep tombstone and replay operations idempotent."""

    def test_upsert_and_same_key_tombstone_are_idempotent(self) -> None:
        key, payload = _alarm_record()
        alarm = decode_alarm(key, payload)
        registry = AlarmRegistry()

        self.assertTrue(registry.upsert(alarm))
        self.assertFalse(registry.upsert(alarm))
        self.assertEqual(registry.alarms, (alarm,))
        self.assertTrue(registry.remove(alarm.identity.alarm_key))
        self.assertFalse(registry.remove(alarm.identity.alarm_key))
        self.assertEqual(registry.alarms, ())