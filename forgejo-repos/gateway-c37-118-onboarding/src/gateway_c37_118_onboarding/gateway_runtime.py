"""Persistent C37.118 version-2 TCP gateway runtime for one reviewed source."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import math
import os
import socket
import sys
from time import sleep, time_ns
from typing import Protocol

from kafka import KafkaProducer
from kafka.errors import KafkaError

from gateway_c37_118_onboarding.c37_118_v2 import (
    COMMAND_REQUEST_CONFIGURATION_2,
    COMMAND_TURN_ON,
    C37_118V2Error,
    FRAME_TYPE_CONFIGURATION_2,
    FRAME_TYPE_DATA,
    MAX_FRAME_BYTES,
    MIN_FRAME_BYTES,
    DataFrame,
    FrameBuffer,
    encode_command,
    decode_data_frame,
    parse_configuration_2,
    parse_frame_header,
)
from gateway_c37_118_onboarding.config import CatalogError, SourceDefinition, load_catalog
from gateway_c37_118_onboarding.normalization import (
    NormalizationError,
    SourceMapping,
    build_source_mapping,
    normalize_data_frame,
)


CONFIGURATION_CHANGE_STAT_BIT = 1 << 10


class GatewayRuntimeError(RuntimeError):
    """Raised when the gateway runtime cannot load or publish its source safely."""


class DeliveryFuture(Protocol):
    """Minimal Kafka delivery interface used by the gateway runtime."""

    def get(self, timeout: float | None = None) -> object:
        """Wait for broker acknowledgement."""


class MeasurementProducer(Protocol):
    """Minimal Kafka producer interface for raw Common Format records."""

    def send(
        self,
        topic: str,
        value: bytes,
        key: bytes,
        timestamp_ms: int,
    ) -> DeliveryFuture:
        """Send one raw-Protobuf measurement record."""


class TcpConnection(Protocol):
    """TCP operations required by the source session."""

    def close(self) -> object:
        """Close the source connection."""

    def recv(self, buffer_size: int) -> bytes:
        """Receive one bounded TCP byte sequence."""

    def sendall(self, data: bytes) -> object:
        """Send one complete C37.118 command frame."""

    def settimeout(self, value: float | None) -> object:
        """Set the socket read timeout."""


@dataclass(frozen=True)
class GatewaySettings:
    """Environment-backed configuration for one isolated gateway instance."""

    kafka_bootstrap_servers: str
    catalog_directory: str
    catalog_id: str
    catalog_revision: str
    source_id: str
    live_measurement_topic: str
    connect_timeout_seconds: float
    read_timeout_seconds: float
    maximum_frame_bytes: int
    reconnect_initial_seconds: float
    reconnect_max_seconds: float

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> GatewaySettings:
        """Read the narrow configuration required by a source-specific adapter."""

        values = os.environ if environment is None else environment
        reconnect_initial_seconds = _positive_float(
            values,
            "WAMA_C37_118_GATEWAY_RECONNECT_INITIAL_SECONDS",
            "1",
        )
        reconnect_max_seconds = _positive_float(
            values,
            "WAMA_C37_118_GATEWAY_RECONNECT_MAX_SECONDS",
            "30",
        )
        if reconnect_max_seconds < reconnect_initial_seconds:
            raise GatewayRuntimeError(
                "WAMA_C37_118_GATEWAY_RECONNECT_MAX_SECONDS must not be below the initial delay"
            )
        return cls(
            kafka_bootstrap_servers=_required(values, "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            catalog_directory=_required(
                values,
                "WAMA_MASTERDATA_CATALOG_DIRECTORY",
                "/app/catalog/sources",
            ),
            catalog_id=_required(
                values,
                "WAMA_MASTERDATA_CATALOG_ID",
                "wama-c37-118-onboarding",
            ),
            catalog_revision=_required(
                values,
                "WAMA_MASTERDATA_CATALOG_REVISION",
                "runtime",
            ),
            source_id=_required(values, "WAMA_C37_118_GATEWAY_SOURCE_ID", ""),
            live_measurement_topic=_required(
                values,
                "WAMA_LIVE_MEASUREMENT_TOPIC",
                "LiveMeasurement",
            ),
            connect_timeout_seconds=_positive_float(
                values,
                "WAMA_C37_118_GATEWAY_CONNECT_TIMEOUT_SECONDS",
                "10",
            ),
            read_timeout_seconds=_positive_float(
                values,
                "WAMA_C37_118_GATEWAY_READ_TIMEOUT_SECONDS",
                "10",
            ),
            maximum_frame_bytes=_frame_size(values),
            reconnect_initial_seconds=reconnect_initial_seconds,
            reconnect_max_seconds=reconnect_max_seconds,
        )


class LiveMeasurementPublisher:
    """Acknowledge every raw-Protobuf Common Format publication before continuing."""

    def __init__(
        self,
        producer: MeasurementProducer,
        topic: str,
        delivery_timeout_seconds: float = 10.0,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._delivery_timeout_seconds = delivery_timeout_seconds

    def publish(self, measurements: tuple[object, ...], timestamp_ms: int) -> int:
        """Publish a decoded source frame in deterministic catalog signal order."""

        count = 0
        for measurement in measurements:
            mrid = getattr(measurement, "mrid")
            payload = measurement.SerializeToString()
            delivery = self._producer.send(
                self._topic,
                key=mrid.encode("utf-8"),
                value=payload,
                timestamp_ms=timestamp_ms,
            )
            delivery.get(timeout=self._delivery_timeout_seconds)
            count += 1
        return count


class GatewaySession:
    """Protocol state for one TCP connection to one configured C37.118 source."""

    def __init__(
        self,
        source: SourceDefinition,
        publisher: LiveMeasurementPublisher,
        clock_ms: Callable[[], int] = lambda: time_ns() // 1_000_000,
        maximum_frame_bytes: int = MAX_FRAME_BYTES,
    ) -> None:
        self._source = source
        self._publisher = publisher
        self._clock_ms = clock_ms
        self._frame_buffer = FrameBuffer(maximum_frame_bytes)
        self._mapping: SourceMapping | None = None

    def configuration_request(self) -> bytes:
        """Build the initial or reconfiguration CFG-2 request command."""

        return self._command(COMMAND_REQUEST_CONFIGURATION_2)

    def receive(self, received: bytes, gateway_timestamp_ms: int) -> tuple[bytes, ...]:
        """Process received frames and return protocol commands that must be sent next."""

        outbound: list[bytes] = []
        for frame in self._frame_buffer.feed(received):
            header = parse_frame_header(frame)
            if header.frame_type == FRAME_TYPE_CONFIGURATION_2:
                configuration = parse_configuration_2(frame)
                self._mapping = build_source_mapping(self._source, configuration)
                outbound.append(self._command(COMMAND_TURN_ON))
                continue
            if header.frame_type != FRAME_TYPE_DATA:
                raise C37_118V2Error("gateway received an unsupported C37.118 frame type")
            if self._mapping is None:
                raise C37_118V2Error("gateway received data before accepting a compatible CFG-2")

            data_frame = decode_data_frame(frame, self._mapping.configuration)
            selected_pmu = data_frame.pmu(self._source.pmu_idcode)
            if selected_pmu.stat & CONFIGURATION_CHANGE_STAT_BIT:
                self._mapping = None
                outbound.append(self.configuration_request())
                continue
            measurements = normalize_data_frame(self._mapping, data_frame, gateway_timestamp_ms)
            self._publisher.publish(measurements, gateway_timestamp_ms)
        return tuple(outbound)

    def _command(self, command: int) -> bytes:
        clock_ms = self._clock_ms()
        if isinstance(clock_ms, bool) or clock_ms < 0:
            raise GatewayRuntimeError("gateway clock must return non-negative milliseconds")
        return encode_command(
            self._source.pmu_idcode,
            command,
            soc=clock_ms // 1_000,
        )


class GatewayRuntime:
    """Reconnect a source-specific session without buffering unacknowledged records."""

    def __init__(
        self,
        settings: GatewaySettings,
        source: SourceDefinition,
        publisher: LiveMeasurementPublisher,
        socket_factory: Callable[[tuple[str, int], float], TcpConnection] = socket.create_connection,
        clock_ms: Callable[[], int] = lambda: time_ns() // 1_000_000,
        sleep_function: Callable[[float], None] = sleep,
        report_error: Callable[[Exception], None] | None = None,
    ) -> None:
        self._settings = settings
        self._source = source
        self._publisher = publisher
        self._socket_factory = socket_factory
        self._clock_ms = clock_ms
        self._sleep = sleep_function
        self._report_error = report_error or _report_error

    def run_forever(self, should_stop: Callable[[], bool] = lambda: False) -> None:
        """Run connection attempts until the service is asked to stop."""

        reconnect_seconds = self._settings.reconnect_initial_seconds
        while not should_stop():
            try:
                self.run_connection()
                reconnect_seconds = self._settings.reconnect_initial_seconds
            except (C37_118V2Error, GatewayRuntimeError, KafkaError, OSError) as error:
                if should_stop():
                    return
                self._report_error(error)
                self._sleep(reconnect_seconds)
                reconnect_seconds = min(
                    reconnect_seconds * 2,
                    self._settings.reconnect_max_seconds,
                )

    def run_connection(self) -> None:
        """Connect once and process frames until the source closes or fails the session."""

        connection = self._socket_factory(
            (self._source.ip_address, self._source.port),
            self._settings.connect_timeout_seconds,
        )
        try:
            connection.settimeout(self._settings.read_timeout_seconds)
            session = GatewaySession(
                self._source,
                self._publisher,
                clock_ms=self._clock_ms,
                maximum_frame_bytes=self._settings.maximum_frame_bytes,
            )
            connection.sendall(session.configuration_request())
            while True:
                received = connection.recv(self._settings.maximum_frame_bytes)
                if not received:
                    raise ConnectionError("C37.118 source closed the TCP connection")
                gateway_timestamp_ms = self._clock_ms()
                for command in session.receive(received, gateway_timestamp_ms):
                    connection.sendall(command)
        finally:
            try:
                connection.close()
            except OSError:
                pass


def load_gateway_source(settings: GatewaySettings) -> SourceDefinition:
    """Load exactly one reviewed source from the synchronized catalog revision."""

    try:
        catalog = load_catalog(
            settings.catalog_directory,
            settings.catalog_id,
            settings.catalog_revision,
        )
    except CatalogError as error:
        raise GatewayRuntimeError(f"C37.118 source catalog is invalid: {error}") from error
    for source in catalog.sources:
        if source.source_id == settings.source_id:
            return source
    raise GatewayRuntimeError(f"C37.118 source {settings.source_id!r} is not in the approved catalog")


def run() -> int:
    """Create one Kafka producer and run the persistent source adapter."""

    try:
        settings = GatewaySettings.from_environment()
        source = load_gateway_source(settings)
    except GatewayRuntimeError as error:
        print(f"C37.118 gateway startup failed: {error}", file=sys.stderr)
        return 1

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
        client_id=f"wama-c37-118-gateway-{source.source_id}",
        acks="all",
        retries=5,
        request_timeout_ms=30_000,
    )
    publisher = LiveMeasurementPublisher(producer, settings.live_measurement_topic)
    runtime = GatewayRuntime(settings, source, publisher)
    try:
        runtime.run_forever()
    except KeyboardInterrupt:
        return 0
    finally:
        producer.close(timeout=30)
    return 0


def _required(values: Mapping[str, str], name: str, default: str) -> str:
    value = values.get(name, default).strip()
    if not value:
        raise GatewayRuntimeError(f"{name} must be configured")
    return value


def _positive_float(values: Mapping[str, str], name: str, default: str) -> float:
    value = _required(values, name, default)
    try:
        parsed = float(value)
    except ValueError as error:
        raise GatewayRuntimeError(f"{name} must be a number") from error
    if not math.isfinite(parsed) or parsed <= 0:
        raise GatewayRuntimeError(f"{name} must be a positive finite number")
    return parsed


def _frame_size(values: Mapping[str, str]) -> int:
    value = _required(values, "WAMA_C37_118_GATEWAY_MAX_FRAME_BYTES", str(MAX_FRAME_BYTES))
    try:
        parsed = int(value)
    except ValueError as error:
        raise GatewayRuntimeError("WAMA_C37_118_GATEWAY_MAX_FRAME_BYTES must be an integer") from error
    if not MIN_FRAME_BYTES <= parsed <= MAX_FRAME_BYTES:
        raise GatewayRuntimeError(
            f"WAMA_C37_118_GATEWAY_MAX_FRAME_BYTES must be in {MIN_FRAME_BYTES}..={MAX_FRAME_BYTES}"
        )
    return parsed


def _report_error(error: Exception) -> None:
    print(f"C37.118 gateway connection failed: {error}", file=sys.stderr)