"""Tests for canonical Masterdata Protobuf encoding."""

from __future__ import annotations

from datetime import datetime, timezone
import unittest

from gateway_c37_118_onboarding.codec import (
    MasterdataCodecError,
    build_source_message,
    decode_source_message,
    serialize_source_message,
)
from gateway_c37_118_onboarding.config import (
    C37_118V2SignalSelectorDefinition,
    SignalDefinition,
    SourceDefinition,
)


class CodecTests(unittest.TestCase):
    """Ensure the runtime representation preserves the approved source mapping."""

    def test_encodes_deterministic_source_projection(self) -> None:
        source = self._source()
        published_at = datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc)

        message = build_source_message(source, "wama-c37-118-onboarding", "abc123", published_at)
        payload = serialize_source_message(message)
        decoded = decode_source_message(source.source_id.encode("utf-8"), payload)

        self.assertEqual(decoded.source_id, source.source_id)
        self.assertEqual(decoded.WhichOneof("connection"), "c37_118_tcp")
        self.assertEqual(decoded.c37_118_tcp.port, 4712)
        self.assertEqual(decoded.c37_118_tcp.wire_version, 2)
        self.assertEqual(decoded.signals[0].mrid, "urn:wama:poc:pmu:bay-01:frequency")
        self.assertEqual(decoded.signals[0].c37_118_v2_selector.WhichOneof("selector"), "frequency")
        self.assertEqual(payload, serialize_source_message(message))

    def test_rejects_a_mismatched_key(self) -> None:
        message = build_source_message(
            self._source(),
            "wama-c37-118-onboarding",
            "abc123",
            datetime(2026, 8, 21, tzinfo=timezone.utc),
        )

        with self.assertRaisesRegex(MasterdataCodecError, "Kafka key"):
            decode_source_message(b"other-source", serialize_source_message(message))

    @staticmethod
    def _source() -> SourceDefinition:
        return SourceDefinition(
            source_id="pmu-bay-01",
            site_id="wama-poc-bay-01",
            display_name="WAMA PoC Bay 01",
            ip_address="192.0.2.10",
            port=4712,
            pmu_idcode=1001,
            wire_version=2,
            signals=(
                SignalDefinition(
                    signal_id="frequency",
                    source_channel="FREQ",
                    mrid="urn:wama:poc:pmu:bay-01:frequency",
                    value_kind="double",
                    quantity="frequency",
                    unit="Hz",
                    c37_118_v2_selector=C37_118V2SignalSelectorDefinition(
                        kind="frequency"
                    ),
                ),
            ),
        )