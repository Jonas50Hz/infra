"""Fixture construction checks for the IEC 104 test control center."""

from __future__ import annotations

from threading import Event, Lock
from unittest.mock import patch
from uuid import UUID
import unittest

import c104
from iec104_common.contract import validate_export_record
from iec104_common.generated import iec104_export_pb2
from iec104_receiver.main import (
    _GENERAL_INTERROGATION,
    ReceiverError,
    Settings,
    _receiver_callback,
    _resolve_ipv4,
    _validate_received,
    build_fixture,
    publish_fixture,
)


class FixtureTests(unittest.TestCase):
    """Keep test fixture records isolated, typed, and contract-valid."""

    def test_builds_three_unique_typed_monitor_records(self) -> None:
        fixture = build_fixture(UUID("4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237"))

        self.assertEqual(len(fixture.records), 3)
        self.assertEqual(len({record.export_id for record in fixture.records}), 3)
        self.assertEqual(
            [record.iec104_asdu.type_id for record in fixture.records],
            [
                iec104_export_pb2.IEC104_TYPE_ID_M_SP_NA_1,
                iec104_export_pb2.IEC104_TYPE_ID_M_DP_NA_1,
                iec104_export_pb2.IEC104_TYPE_ID_M_ME_NC_1,
            ],
        )
        for record in fixture.records:
            validate_export_record(record)
            self.assertEqual(record.iec104_asdu.common_address, fixture.common_address)
            self.assertEqual(record.iec104_asdu.cause.code, 3)

    def test_rejects_missing_or_mismatched_received_values(self) -> None:
        fixture = build_fixture(UUID("4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237"))

        with self.assertRaisesRegex(ReceiverError, "unexpected IEC 104 point set"):
            _validate_received(fixture.expected, ())

    @patch("iec104_receiver.main.socket.gethostbyname", return_value="172.18.0.20")
    def test_resolves_compose_service_name_for_c104(self, resolve: object) -> None:
        self.assertEqual(_resolve_ipv4("iec104-exporter"), "172.18.0.20")
        self.assertIsNotNone(resolve)

    def test_receiver_callback_has_concrete_c104_annotations(self) -> None:
        callback = _receiver_callback(
            iec104_export_pb2.IEC104_TYPE_ID_M_SP_NA_1,
            {},
            Lock(),
            Event(),
            set(),
        )

        self.assertIs(callback.__annotations__["point"], c104.Point)
        self.assertIs(callback.__annotations__["previous_info"], c104.Information)
        self.assertIs(callback.__annotations__["message"], c104.IncomingMessage)
        self.assertIs(callback.__annotations__["return"], c104.ResponseState)

    def test_general_interrogation_fixture_is_an_iec_application_frame(self) -> None:
        self.assertEqual(_GENERAL_INTERROGATION[:2], b"\x68\x0e")
        self.assertEqual(_GENERAL_INTERROGATION[2] & 0x01, 0)

    def test_publish_only_mode_sends_fixture_without_an_iec_connection(self) -> None:
        fixture = build_fixture(UUID("4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237"))
        producer = _Producer()
        settings = Settings(
            exporter_host="iec104-exporter",
            exporter_port=2404,
            kafka_bootstrap_servers="kafka:9092",
            kafka_topic="Export",
            mode="publish-only",
            timeout_seconds=30,
        )

        publish_fixture(settings, fixture, lambda **kwargs: producer)

        self.assertTrue(producer.closed)
        self.assertEqual(len(producer.records), 3)
        self.assertEqual(
            [record["key"] for record in producer.records],
            [record.export_id.encode("utf-8") for record in fixture.records],
        )

    def test_rejects_unknown_receiver_mode(self) -> None:
        with self.assertRaisesRegex(ReceiverError, "must be verify or publish-only"):
            Settings.from_environment({"IEC104_RECEIVER_MODE": "invalid"})


class _Future:
    def get(self, timeout: float) -> None:
        del timeout


class _Producer:
    def __init__(self) -> None:
        self.closed = False
        self.records: list[dict[str, object]] = []

    def send(self, topic: str, **kwargs: object) -> _Future:
        self.records.append({"topic": topic, **kwargs})
        return _Future()

    def close(self) -> None:
        self.closed = True


if __name__ == "__main__":
    unittest.main()