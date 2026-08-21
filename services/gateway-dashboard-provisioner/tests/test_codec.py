"""Tests for trusted Masterdata-to-dashboard source decoding."""

from __future__ import annotations

from datetime import datetime, timezone
import unittest

from gateway_dashboard_provisioner.codec import (
    MasterdataDecodeError,
    decode_source,
    decode_source_id,
)
from gateway_dashboard_provisioner.generated import masterdata_pb2


class MasterdataCodecTests(unittest.TestCase):
    """Reject anything that cannot safely define a generated gateway page."""

    def test_decodes_a_valid_c37_source(self) -> None:
        message = _source_message()

        source = decode_source(b"pmu-bay-01", message.SerializeToString())

        self.assertEqual(source.source_id, "pmu-bay-01")
        self.assertEqual(source.signals[0].signal_id, "frequency")
        self.assertEqual(source.published_at, datetime(2026, 8, 21, 12, tzinfo=timezone.utc))

    def test_rejects_mismatched_key_and_source_identity(self) -> None:
        with self.assertRaisesRegex(MasterdataDecodeError, "Kafka key"):
            decode_source(b"pmu-bay-02", _source_message().SerializeToString())

    def test_rejects_unsupported_signal_mapping(self) -> None:
        message = _source_message()
        message.signals[0].unit = "mHz"

        with self.assertRaisesRegex(MasterdataDecodeError, "quantity mapping"):
            decode_source(b"pmu-bay-01", message.SerializeToString())

    def test_validates_tombstone_keys(self) -> None:
        self.assertEqual(decode_source_id(b"pmu-bay-01"), "pmu-bay-01")
        with self.assertRaisesRegex(MasterdataDecodeError, "unsupported format"):
            decode_source_id(b"PMU Bay 01")


def _source_message() -> masterdata_pb2.SourceMasterdata:
    message = masterdata_pb2.SourceMasterdata(
        source_id="pmu-bay-01",
        catalog_id="wama-c37-118-onboarding",
        catalog_revision="abc123",
    )
    message.published_at.FromDatetime(datetime(2026, 8, 21, 12, tzinfo=timezone.utc))
    message.location.site_id = "wama-poc-bay-01"
    message.location.display_name = "WAMA PoC Bay 01"
    message.c37_118_tcp.ip_address = "192.0.2.10"
    message.c37_118_tcp.port = 4712
    message.c37_118_tcp.pmu_idcode = 1001
    signal = message.signals.add()
    signal.signal_id = "frequency"
    signal.source_channel = "FREQ"
    signal.mrid = "urn:wama:poc:pmu:bay-01:frequency"
    signal.value_kind = masterdata_pb2.MCCS_VALUE_KIND_DOUBLE
    signal.quantity = "frequency"
    signal.unit = "Hz"
    return message