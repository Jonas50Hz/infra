"""Tests for canonical raw-Protobuf Alarm decoding and Alerta mapping."""

from __future__ import annotations

from datetime import datetime, timezone
import unittest

from alarm_alerta_ingress.codec import AlarmDecodeError, canonical_alarm_key, decode_alarm
from alarm_alerta_ingress.generated import alarm_pb2
from alarm_alerta_ingress.model import (
    WAMA_MANAGED_BY_ATTRIBUTE,
    WAMA_MANAGED_BY_VALUE,
    WAMA_MANAGED_TAG,
)


class AlarmCodecTests(unittest.TestCase):
    """Require exact key/value identity before reaching Alerta."""

    def test_decodes_canonical_alarm_and_uses_fixed_native_severity(self) -> None:
        key, payload = _alarm_record()

        alarm = decode_alarm(key, payload)
        alerta_payload = alarm.alerta_payload()

        self.assertEqual(alarm.identity.alarm_key, key.decode("utf-8"))
        self.assertEqual(alarm.severity, "WARNING")
        self.assertEqual(alerta_payload["severity"], "indeterminate")
        self.assertEqual(alerta_payload["text"], "[WAMA WARNING] Frequency exceeds threshold")
        self.assertIn(WAMA_MANAGED_TAG, alerta_payload["tags"])
        self.assertEqual(
            alerta_payload["attributes"][WAMA_MANAGED_BY_ATTRIBUTE],
            WAMA_MANAGED_BY_VALUE,
        )

    def test_rejects_value_with_mismatched_canonical_identity(self) -> None:
        key, payload = _alarm_record()
        message = alarm_pb2.AlarmDesiredState()
        message.ParseFromString(payload)
        message.mrid = "urn:wama:poc:pmu:other:frequency"

        with self.assertRaisesRegex(AlarmDecodeError, "identity"):
            decode_alarm(key, message.SerializeToString())

    def test_rejects_noncanonical_episode_identifier(self) -> None:
        key, payload = _alarm_record()
        message = alarm_pb2.AlarmDesiredState()
        message.ParseFromString(payload)
        message.episode_id = "A0D5E631-962D-4CE3-86BA-04D4252A3285"

        with self.assertRaisesRegex(AlarmDecodeError, "canonical UUID"):
            decode_alarm(key, message.SerializeToString())


def _alarm_record() -> tuple[bytes, bytes]:
    rule_id = "frequency-high"
    mrid = "urn:wama:poc:pmu:bay-01:frequency"
    alarm_key = canonical_alarm_key(rule_id, mrid)
    message = alarm_pb2.AlarmDesiredState(
        alarm_key=alarm_key,
        episode_id="a0d5e631-962d-4ce3-86ba-04d4252a3285",
        rule_id=rule_id,
        mrid=mrid,
        severity=alarm_pb2.ALARM_SEVERITY_WARNING,
        rule_revision="2026-08-26",
    )
    timestamp = datetime(2026, 8, 26, 12, tzinfo=timezone.utc)
    message.activated_at.FromDatetime(timestamp)
    message.current_evidence.observed_at.FromDatetime(timestamp)
    message.current_evidence.summary = "Frequency exceeds threshold"
    attribute = message.current_evidence.attributes.add()
    attribute.name = "threshold"
    attribute.value = "50.2"
    return alarm_key.encode("utf-8"), message.SerializeToString()