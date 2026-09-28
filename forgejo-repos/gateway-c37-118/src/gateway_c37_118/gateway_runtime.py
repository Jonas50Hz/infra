"""Source-scoped C37.118 v2 TCP adapter publishing normalized measurements."""

from __future__ import annotations

from datetime import datetime, timezone
import logging
import os
import socket
from time import sleep, time

from google.protobuf.timestamp_pb2 import Timestamp
from kafka import KafkaProducer

from gateway_c37_118.c37_v2 import C37V2Error, command_frame, parse_cfg2, parse_data
from gateway_c37_118.catalog import CatalogError, Signal, Source, load_catalog
from gateway_c37_118.generated import rtd_schema_pb2

LOGGER = logging.getLogger(__name__)
MAX_SOURCE_TIMESTAMP_LEAD_SECONDS = 0.25


def _read_frame(connection: socket.socket) -> bytes:
    header = _read_exact(connection, 4)
    size = int.from_bytes(header[2:4], "big")
    if not 16 <= size <= 4096:
        raise C37V2Error("unsafe C37.118 frame size")
    return header + _read_exact(connection, size - 4)


def _read_exact(connection: socket.socket, count: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < count:
        received = connection.recv(count - len(chunks))
        if not received:
            raise ConnectionError("C37.118 endpoint closed the connection")
        chunks.extend(received)
    return bytes(chunks)


def _timestamp(
    seconds: int,
    fraction: int,
    time_base: int,
    *,
    now: float | None = None,
) -> Timestamp:
    timestamp = Timestamp(seconds=seconds, nanos=(fraction * 1_000_000_000) // time_base)
    current_time = time() if now is None else now
    if (
        timestamp.seconds + timestamp.nanos / 1_000_000_000
        > current_time + MAX_SOURCE_TIMESTAMP_LEAD_SECONDS
    ):
        raise C37V2Error("C37.118 source timestamp exceeds the future tolerance")
    return timestamp


def measurement(signal: Signal, value: float, field: Timestamp, stat: int) -> rtd_schema_pb2.MCCSMeasurementValue:
    """Normalize one decoded scalar into the Common Format wire contract."""

    now = datetime.now(timezone.utc)
    receipt = Timestamp()
    receipt.FromDatetime(now)
    record = rtd_schema_pb2.MCCSMeasurementValue(mrid=signal.mrid, double_value=value)
    record.timestamp_field.CopyFrom(field)
    record.timestamp_gateway.CopyFrom(receipt)
    record.timestamp_mccs.CopyFrom(receipt)
    record.quality.valid = stat == 0
    record.quality.substituted = bool(stat & (1 << 9))
    return record


def _publish_data(producer: KafkaProducer, source: Source, configuration, frame) -> None:
    decoded = parse_data(frame, configuration)
    field = _timestamp(decoded.source_seconds, decoded.source_fraction, configuration.time_base)
    for signal in source.signals:
        if signal.selector.startswith("phasor:"):
            value = decoded.phasors.get(signal.selector.removeprefix("phasor:"))
            if value is None:
                raise C37V2Error(f"CFG-2 omitted mapped channel {signal.source_channel}")
        elif signal.selector == "frequency":
            value = decoded.frequency
        else:
            value = decoded.rocof
        record = measurement(signal, value, field, decoded.stat)
        producer.send(
            os.environ.get("WAMA_LIVE_MEASUREMENT_TOPIC", "LiveMeasurement"),
            key=signal.mrid.encode("utf-8"),
            value=record.SerializeToString(deterministic=True),
            timestamp_ms=record.timestamp_mccs.seconds * 1000 + record.timestamp_mccs.nanos // 1_000_000,
        ).get(timeout=10)


def run_source(source: Source) -> None:
    """Reconnect forever with bounded backoff for one approved source only."""

    producer = KafkaProducer(
        bootstrap_servers=os.environ.get("WAMA_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(","),
        acks="all", retries=10, client_id=f"c37-118-gateway-{source.source_id}",
    )
    try:
        while True:
            try:
                with socket.create_connection((source.ip_address, source.port), timeout=5) as connection:
                    connection.settimeout(5)
                    connection.sendall(command_frame(source.pmu_idcode, 5))
                    configuration = parse_cfg2(_read_frame(connection), source.pmu_idcode)
                    connection.sendall(command_frame(source.pmu_idcode, 2))
                    while True:
                        _publish_data(producer, source, configuration, _read_frame(connection))
            except (ConnectionError, OSError, C37V2Error) as error:
                LOGGER.warning("Source %s reconnecting after: %s", source.source_id, error)
                sleep(1)
    finally:
        producer.close()


def main() -> None:
    """Start exactly the source chosen by the guarded generated Compose overlay."""

    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO").upper())
    source_id = os.environ.get("WAMA_SOURCE_ID", "")
    try:
        if not source_id:
            raise CatalogError("WAMA_SOURCE_ID is required")
        run_source(load_catalog(os.environ.get("WAMA_CATALOG_DIR", "/app/catalog")).source(source_id))
    except CatalogError as error:
        raise SystemExit(str(error)) from error


if __name__ == "__main__":
    main()
