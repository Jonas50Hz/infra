"""Receive one unique IEC 104 fixture set as a minimal control center."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Event, Lock
import math
import os
import socket
import time
from typing import Any
from uuid import UUID, uuid4, uuid5

import c104
from google.protobuf.timestamp_pb2 import Timestamp
from kafka import KafkaProducer
from kafka.errors import KafkaError

from iec104_common.contract import ContractValidationError, validate_export_record
from iec104_common.generated import iec104_export_pb2


class ReceiverError(RuntimeError):
    """Raised when the test control center cannot prove outbound IEC 104 data."""


@dataclass(frozen=True)
class Settings:
    """Minimal connections owned by the profile-gated receiver."""

    exporter_host: str
    exporter_port: int
    kafka_bootstrap_servers: str
    kafka_topic: str
    mode: str
    timeout_seconds: float

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> "Settings":
        """Load local-PoC receiver settings."""

        values = os.environ if environment is None else environment
        return cls(
            exporter_host=_required(values, "IEC104_EXPORTER_HOST", "iec104-exporter"),
            exporter_port=_port(values, "IEC104_EXPORTER_PORT", 2404),
            kafka_bootstrap_servers=_required(values, "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            kafka_topic=_required(values, "KAFKA_TOPIC", "Export"),
            mode=_mode(values),
            timeout_seconds=_positive_float(values, "IEC104_RECEIVER_TIMEOUT_SECONDS", 30.0),
        )


@dataclass(frozen=True)
class ExpectedValue:
    """One monitor point that the receiver must observe from c104."""

    information_object_address: int
    quality: int
    type_id: int
    value: bool | int | float


@dataclass(frozen=True)
class Fixture:
    """Unique test records and their expected IEC 104 observations."""

    common_address: int
    expected: tuple[ExpectedValue, ...]
    records: tuple[iec104_export_pb2.ExportRecord, ...]


@dataclass(frozen=True)
class ReceivedValue:
    """A decoded c104 monitor-direction callback."""

    cause: int
    information_object_address: int
    quality: int
    type_id: int
    value: bool | int | float


_C104_TYPES = {
    iec104_export_pb2.IEC104_TYPE_ID_M_SP_NA_1: c104.Type.M_SP_NA_1,
    iec104_export_pb2.IEC104_TYPE_ID_M_DP_NA_1: c104.Type.M_DP_NA_1,
    iec104_export_pb2.IEC104_TYPE_ID_M_ME_NC_1: c104.Type.M_ME_NC_1,
}
_FIXTURE_NAMESPACE = UUID("8043f6f0-f9ec-4b07-b85c-310a1f4f6d20")
_GENERAL_INTERROGATION = bytes.fromhex("680e0000000064010600000000000014")
_START_DATA_TRANSFER_ACTIVATION = bytes.fromhex("680407000000")
_START_DATA_TRANSFER_CONFIRMATION = bytes.fromhex("68040b000000")


def main() -> None:
    """Publish one fixture only after the control center has started transfer."""

    try:
        settings = Settings.from_environment()
        fixture = build_fixture(uuid4())
        if settings.mode == "publish-only":
            publish_fixture(settings, fixture)
            print(f"IEC 104 fixture published for common address {fixture.common_address}")
            return
        received = receive_fixture(settings, fixture)
        verify_application_input_rejected(settings)
        print(
            "IEC 104 receiver verified "
            f"{len(received)} outbound monitor values and rejected application input "
            f"for common address {fixture.common_address}"
        )
    except (ContractValidationError, KafkaError, OSError, ReceiverError, ValueError) as error:
        raise SystemExit(f"IEC 104 receiver test failed: {error}") from error


def build_fixture(run_id: UUID) -> Fixture:
    """Create three independently keyed records for an isolated test station."""

    common_address = 1 + run_id.int % 65_535
    first_address = 1 + (run_id.int >> 16) % (0xFFFFFF - 3)
    timestamp = _timestamp(datetime.now(timezone.utc))
    expected = (
        ExpectedValue(
            information_object_address=first_address,
            quality=32,
            type_id=iec104_export_pb2.IEC104_TYPE_ID_M_SP_NA_1,
            value=True,
        ),
        ExpectedValue(
            information_object_address=first_address + 1,
            quality=16,
            type_id=iec104_export_pb2.IEC104_TYPE_ID_M_DP_NA_1,
            value=iec104_export_pb2.IEC104_DOUBLE_POINT_ON,
        ),
        ExpectedValue(
            information_object_address=first_address + 2,
            quality=1,
            type_id=iec104_export_pb2.IEC104_TYPE_ID_M_ME_NC_1,
            value=50.01,
        ),
    )
    records = tuple(
        _record(run_id, index, common_address, timestamp, expected_value)
        for index, expected_value in enumerate(expected)
    )
    for record in records:
        validate_export_record(record)
    return Fixture(common_address=common_address, expected=expected, records=records)


def receive_fixture(
    settings: Settings,
    fixture: Fixture,
    client_factory: Callable[[], Any] = c104.Client,
    producer_factory: Callable[..., Any] = KafkaProducer,
) -> tuple[ReceivedValue, ...]:
    """Connect, start data transfer, publish fixtures, and validate callbacks."""

    received: dict[tuple[int, int], ReceivedValue] = {}
    received_event = Event()
    received_lock = Lock()
    client = client_factory()
    connection = client.add_connection(
        ip=_resolve_ipv4(settings.exporter_host),
        port=settings.exporter_port,
        init=c104.Init.NONE,
    )
    if connection is None:
        raise ReceiverError("c104 did not create the exporter connection")
    station = connection.add_station(common_address=fixture.common_address)
    if station is None:
        raise ReceiverError("c104 did not create the test station")

    expected_keys = {
        (expected_value.type_id, expected_value.information_object_address)
        for expected_value in fixture.expected
    }
    for expected_value in fixture.expected:
        point = station.add_point(
            io_address=expected_value.information_object_address,
            type=_C104_TYPES[expected_value.type_id],
        )
        if point is None:
            raise ReceiverError("c104 did not create a test information object")
        point.on_receive(
            _receiver_callback(
                expected_value.type_id,
                received,
                received_lock,
                received_event,
                expected_keys,
            )
        )

    try:
        client.start()
        _wait_for(
            lambda: bool(client.has_active_connections),
            received_event,
            settings.timeout_seconds,
            "control center did not start IEC 104 data transfer",
        )
        publish_fixture(settings, fixture, producer_factory)
        _wait_for(
            lambda: len(received) == len(expected_keys),
            received_event,
            settings.timeout_seconds,
            "control center did not receive every fixture value",
        )
    finally:
        client.stop()

    ordered_received = tuple(received[key] for key in sorted(received))
    _validate_received(fixture.expected, ordered_received)
    return ordered_received


def publish_fixture(
    settings: Settings,
    fixture: Fixture,
    producer_factory: Callable[..., Any] = KafkaProducer,
) -> None:
    """Publish only fixture records without opening an IEC 104 control center."""

    producer = producer_factory(
        bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
        client_id="iec104-receiver-fixture",
    )
    try:
        for record in fixture.records:
            producer.send(
                settings.kafka_topic,
                key=record.export_id.encode("utf-8"),
                value=record.SerializeToString(),
                timestamp_ms=_timestamp_milliseconds(record.created_at),
            ).get(timeout=settings.timeout_seconds)
    finally:
        producer.close()


def verify_application_input_rejected(settings: Settings) -> None:
    """Require the output-only exporter to close, not answer, an I-frame request."""

    deadline = time.monotonic() + settings.timeout_seconds
    endpoint = (_resolve_ipv4(settings.exporter_host), settings.exporter_port)
    last_error: OSError | None = None
    while time.monotonic() < deadline:
        connection: socket.socket | None = None
        started_data_transfer = False
        try:
            connection = socket.create_connection(endpoint, timeout=2)
            connection.settimeout(2)
            connection.sendall(_START_DATA_TRANSFER_ACTIVATION)
            if _receive_exact(connection, len(_START_DATA_TRANSFER_CONFIRMATION)) != _START_DATA_TRANSFER_CONFIRMATION:
                raise OSError("IEC 104 exporter did not confirm STARTDT")
            started_data_transfer = True
            connection.sendall(_GENERAL_INTERROGATION)
            response = connection.recv(4096)
            if response:
                raise ReceiverError("IEC 104 exporter answered an incoming application frame")
            return
        except ConnectionResetError:
            if started_data_transfer:
                return
            last_error = ConnectionResetError("IEC 104 exporter reset the connection before STARTDT")
        except socket.timeout as error:
            if started_data_transfer:
                raise ReceiverError("IEC 104 exporter did not close an incoming application frame") from error
            last_error = error
        except OSError as error:
            if started_data_transfer:
                return
            last_error = error
        finally:
            if connection is not None:
                connection.close()
        Event().wait(0.1)
    raise ReceiverError("IEC 104 exporter did not become available for application-frame rejection") from last_error


def _receiver_callback(
    type_id: int,
    received: dict[tuple[int, int], ReceivedValue],
    received_lock: Lock,
    received_event: Event,
    expected_keys: set[tuple[int, int]],
) -> Callable[[c104.Point, c104.Information, c104.IncomingMessage], c104.ResponseState]:
    def on_receive(
        point: c104.Point,
        previous_info: c104.Information,
        message: c104.IncomingMessage,
    ) -> c104.ResponseState:
        del previous_info
        value: bool | int | float
        if type_id == iec104_export_pb2.IEC104_TYPE_ID_M_DP_NA_1:
            value = point.value.value
        elif type_id == iec104_export_pb2.IEC104_TYPE_ID_M_ME_NC_1:
            value = float(point.value)
        else:
            value = bool(point.value)
        received_value = ReceivedValue(
            cause=message.cot.value,
            information_object_address=point.io_address,
            quality=point.quality.value,
            type_id=type_id,
            value=value,
        )
        with received_lock:
            received[(type_id, point.io_address)] = received_value
            if expected_keys.issubset(received):
                received_event.set()
        return c104.ResponseState.NONE

    return on_receive


def _validate_received(
    expected_values: tuple[ExpectedValue, ...],
    received_values: tuple[ReceivedValue, ...],
) -> None:
    expected_by_key = {
        (expected.type_id, expected.information_object_address): expected
        for expected in expected_values
    }
    received_by_key = {
        (received.type_id, received.information_object_address): received
        for received in received_values
    }
    if expected_by_key.keys() != received_by_key.keys():
        raise ReceiverError("control center received an unexpected IEC 104 point set")
    for key, expected in expected_by_key.items():
        received = received_by_key[key]
        if received.cause != 3:
            raise ReceiverError("control center received an unexpected cause of transmission")
        if received.quality != expected.quality:
            raise ReceiverError("control center received an unexpected quality descriptor")
        if isinstance(expected.value, float):
            if not isinstance(received.value, float) or not math.isclose(
                received.value,
                expected.value,
                rel_tol=0,
                abs_tol=0.0001,
            ):
                raise ReceiverError("control center received an unexpected short-float value")
        elif received.value != expected.value:
            raise ReceiverError("control center received an unexpected point value")


def _record(
    run_id: UUID,
    index: int,
    common_address: int,
    timestamp: Timestamp,
    expected: ExpectedValue,
) -> iec104_export_pb2.ExportRecord:
    record = iec104_export_pb2.ExportRecord(
        export_id=str(uuid5(_FIXTURE_NAMESPACE, f"{run_id}:{index}")),
    )
    record.created_at.CopyFrom(timestamp)
    asdu = record.iec104_asdu
    asdu.type_id = expected.type_id
    asdu.common_address = common_address
    asdu.cause.code = 3
    information_object = asdu.information_objects.add()
    information_object.information_object_address = expected.information_object_address
    if expected.type_id == iec104_export_pb2.IEC104_TYPE_ID_M_SP_NA_1:
        information_object.single_point.value = bool(expected.value)
        information_object.single_point.quality.substituted = True
    elif expected.type_id == iec104_export_pb2.IEC104_TYPE_ID_M_DP_NA_1:
        information_object.double_point.value = int(expected.value)
        information_object.double_point.quality.blocked = True
    else:
        information_object.short_float.value = float(expected.value)
        information_object.short_float.quality.overflow = True
    return record


def _wait_for(
    condition: Callable[[], bool],
    event: Event,
    timeout_seconds: float,
    message: str,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    while not condition():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ReceiverError(message)
        event.wait(min(remaining, 0.1))
        event.clear()


def _receive_exact(connection: socket.socket, byte_count: int) -> bytes:
    payload = b""
    while len(payload) < byte_count:
        chunk = connection.recv(byte_count - len(payload))
        if not chunk:
            return payload
        payload += chunk
    return payload


def _required(values: Mapping[str, str], name: str, default: str) -> str:
    value = values.get(name, default).strip()
    if not value:
        raise ReceiverError(f"{name} must not be empty")
    return value


def _port(values: Mapping[str, str], name: str, default: int) -> int:
    try:
        value = int(values.get(name, str(default)))
    except ValueError as error:
        raise ReceiverError(f"{name} must be an integer") from error
    if not 1 <= value <= 65_535:
        raise ReceiverError(f"{name} must be between 1 and 65535")
    return value


def _positive_float(values: Mapping[str, str], name: str, default: float) -> float:
    try:
        value = float(values.get(name, str(default)))
    except ValueError as error:
        raise ReceiverError(f"{name} must be a number") from error
    if value <= 0:
        raise ReceiverError(f"{name} must be greater than zero")
    return value


def _mode(values: Mapping[str, str]) -> str:
    value = _required(values, "IEC104_RECEIVER_MODE", "verify")
    if value not in {"verify", "publish-only"}:
        raise ReceiverError("IEC104_RECEIVER_MODE must be verify or publish-only")
    return value


def _resolve_ipv4(host: str) -> str:
    try:
        return socket.gethostbyname(host)
    except OSError as error:
        raise ReceiverError(f"IEC 104 exporter host cannot be resolved: {host}") from error


def _timestamp(value: datetime) -> Timestamp:
    timestamp = Timestamp()
    timestamp.FromDatetime(value)
    return timestamp


def _timestamp_milliseconds(timestamp: Timestamp) -> int:
    return timestamp.seconds * 1_000 + timestamp.nanos // 1_000_000


if __name__ == "__main__":
    main()