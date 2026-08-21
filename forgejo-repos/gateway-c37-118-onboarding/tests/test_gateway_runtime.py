"""Tests for the per-source C37.118 TCP gateway protocol session."""

from __future__ import annotations

import struct
import unittest

from gateway_c37_118_onboarding.c37_118_v2 import (
    COMMAND_REQUEST_CONFIGURATION_2,
    COMMAND_TURN_ON,
    C37_118V2Error,
    FRAME_TYPE_CONFIGURATION_2,
    FRAME_TYPE_DATA,
    crc16_ccitt,
)
from gateway_c37_118_onboarding.config import (
    C37_118V2SignalSelectorDefinition,
    SignalDefinition,
    SourceDefinition,
)
from gateway_c37_118_onboarding.gateway_runtime import (
    GatewayRuntime,
    GatewaySession,
    GatewaySettings,
    LiveMeasurementPublisher,
)
from gateway_c37_118_onboarding.generated import rtd_schema_pb2


class GatewaySessionTests(unittest.TestCase):
    """Ensure the session only publishes after accepting the reviewed CFG-2 mapping."""

    def test_requests_config_starts_after_mapping_and_publishes_measurements(self) -> None:
        producer = _Producer()
        publisher = LiveMeasurementPublisher(producer, "LiveMeasurement")
        session = GatewaySession(_source(), publisher, clock_ms=lambda: 1_700_000_000_600)

        request = session.configuration_request()
        start_commands = session.receive(_configuration_frame(), 1_700_000_000_600)
        data_commands = session.receive(_data_frame(), 1_700_000_000_600)

        self.assertEqual(int.from_bytes(request[14:16], "big"), COMMAND_REQUEST_CONFIGURATION_2)
        self.assertEqual(len(start_commands), 1)
        self.assertEqual(int.from_bytes(start_commands[0][14:16], "big"), COMMAND_TURN_ON)
        self.assertEqual(data_commands, ())
        self.assertEqual(len(producer.records), 1)
        topic, key, payload, timestamp_ms = producer.records[0]
        measurement = rtd_schema_pb2.MCCSMeasurementValue()
        measurement.ParseFromString(payload)
        self.assertEqual(topic, "LiveMeasurement")
        self.assertEqual(key, b"urn:wama:test:frequency")
        self.assertEqual(timestamp_ms, 1_700_000_000_600)
        self.assertAlmostEqual(measurement.double_value, 50.01, places=5)

    def test_pauses_data_on_a_configuration_change_flag(self) -> None:
        producer = _Producer()
        session = GatewaySession(
            _source(),
            LiveMeasurementPublisher(producer, "LiveMeasurement"),
            clock_ms=lambda: 1_700_000_000_600,
        )
        session.receive(_configuration_frame(), 1_700_000_000_600)

        commands = session.receive(_data_frame(stat=CONFIGURATION_CHANGE_STAT), 1_700_000_000_600)

        self.assertEqual(len(commands), 1)
        self.assertEqual(int.from_bytes(commands[0][14:16], "big"), COMMAND_REQUEST_CONFIGURATION_2)
        self.assertEqual(producer.records, [])
        with self.assertRaisesRegex(C37_118V2Error, "before accepting"):
            session.receive(_data_frame(), 1_700_000_000_600)

    def test_runtime_connects_requests_configuration_and_closes_after_eof(self) -> None:
        producer = _Producer()
        connection = _Connection([_configuration_frame(), _data_frame(), b""])
        runtime = GatewayRuntime(
            _settings(),
            _source(),
            LiveMeasurementPublisher(producer, "LiveMeasurement"),
            socket_factory=lambda address, timeout: connection,
            clock_ms=lambda: 1_700_000_000_600,
            report_error=lambda error: None,
        )

        with self.assertRaisesRegex(ConnectionError, "closed"):
            runtime.run_connection()

        self.assertTrue(connection.closed)
        self.assertEqual(len(connection.sent), 2)
        self.assertEqual(
            int.from_bytes(connection.sent[0][14:16], "big"),
            COMMAND_REQUEST_CONFIGURATION_2,
        )
        self.assertEqual(int.from_bytes(connection.sent[1][14:16], "big"), COMMAND_TURN_ON)
        self.assertEqual(len(producer.records), 1)


CONFIGURATION_CHANGE_STAT = 1 << 10


class _Delivery:
    def get(self, timeout: float | None = None) -> object:
        return object()


class _Producer:
    def __init__(self) -> None:
        self.records: list[tuple[str, bytes, bytes, int]] = []

    def send(self, topic: str, key: bytes, value: bytes, timestamp_ms: int) -> _Delivery:
        self.records.append((topic, key, value, timestamp_ms))
        return _Delivery()


class _Connection:
    def __init__(self, received: list[bytes]) -> None:
        self._received = received
        self.closed = False
        self.sent: list[bytes] = []

    def close(self) -> None:
        self.closed = True

    def recv(self, buffer_size: int) -> bytes:
        return self._received.pop(0)

    def sendall(self, data: bytes) -> None:
        self.sent.append(data)

    def settimeout(self, value: float | None) -> None:
        return None


def _source() -> SourceDefinition:
    return SourceDefinition(
        source_id="pmu-test",
        site_id="wama-test",
        display_name="WAMA Test PMU",
        ip_address="192.0.2.10",
        port=4712,
        pmu_idcode=1001,
        wire_version=2,
        signals=(
            SignalDefinition(
                signal_id="frequency",
                source_channel="FREQ",
                mrid="urn:wama:test:frequency",
                value_kind="double",
                quantity="frequency",
                unit="Hz",
                c37_118_v2_selector=C37_118V2SignalSelectorDefinition(kind="frequency"),
            ),
        ),
    )


def _settings() -> GatewaySettings:
    return GatewaySettings(
        kafka_bootstrap_servers="kafka:9092",
        catalog_directory="/app/catalog/sources",
        catalog_id="wama-c37-118-onboarding",
        catalog_revision="test",
        source_id="pmu-test",
        live_measurement_topic="LiveMeasurement",
        connect_timeout_seconds=10.0,
        read_timeout_seconds=10.0,
        maximum_frame_bytes=65_535,
        reconnect_initial_seconds=1.0,
        reconnect_max_seconds=30.0,
    )


def _configuration_frame() -> bytes:
    payload = bytearray()
    payload.extend(struct.pack(">IH", 1_000_000, 1))
    payload.extend(b"PMU-ONE".ljust(16, b" "))
    payload.extend(struct.pack(">HHHHH", 1001, 0x0008, 0, 0, 0))
    payload.extend(struct.pack(">HHh", 1, 3, 50))
    return _frame(FRAME_TYPE_CONFIGURATION_2, bytes(payload))


def _data_frame(stat: int = 0) -> bytes:
    return _frame(FRAME_TYPE_DATA, struct.pack(">Hff", stat, 50.01, 0.0), fracsec=500_000)


def _frame(frame_type: int, payload: bytes, fracsec: int = 0) -> bytes:
    frame_size = 14 + len(payload) + 2
    frame = bytearray(
        struct.pack(
            ">BBHHII",
            0xAA,
            (frame_type << 4) | 2,
            frame_size,
            1001,
            1_700_000_000,
            fracsec,
        )
    )
    frame.extend(payload)
    frame.extend(struct.pack(">H", crc16_ccitt(frame)))
    return bytes(frame)