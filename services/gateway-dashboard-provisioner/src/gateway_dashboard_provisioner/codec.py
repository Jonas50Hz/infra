"""Defensive decoding of raw-Protobuf Masterdata gateway records."""

from __future__ import annotations

from datetime import timezone
from ipaddress import ip_address
import re

from google.protobuf.message import DecodeError

from gateway_dashboard_provisioner.generated import masterdata_pb2
from gateway_dashboard_provisioner.model import GatewaySignal, GatewaySource


_IDENTIFIER = re.compile(r"[a-z0-9][a-z0-9-]{0,62}\Z")
_MRID = re.compile(r"urn:[A-Za-z0-9][A-Za-z0-9:._-]{1,255}\Z")
_QUANTITY_RULES = {
    "voltage": ("V", masterdata_pb2.MCCS_VALUE_KIND_DOUBLE),
    "current": ("A", masterdata_pb2.MCCS_VALUE_KIND_DOUBLE),
    "frequency": ("Hz", masterdata_pb2.MCCS_VALUE_KIND_DOUBLE),
    "rocof": ("Hz/s", masterdata_pb2.MCCS_VALUE_KIND_DOUBLE),
}


class MasterdataDecodeError(ValueError):
    """Raised when a Masterdata record is unsuitable for dashboard rendering."""


def decode_source(key: bytes | None, payload: bytes | None) -> GatewaySource:
    """Decode one non-null source record and enforce its V1 dashboard contract."""

    source_id = decode_source_id(key)
    if payload is None:
        raise MasterdataDecodeError("Masterdata source record has no payload")
    message = masterdata_pb2.SourceMasterdata()
    try:
        message.ParseFromString(payload)
    except DecodeError as error:
        raise MasterdataDecodeError("Masterdata value is not valid raw Protobuf") from error
    if message.source_id != source_id:
        raise MasterdataDecodeError("Masterdata Kafka key must equal message.source_id")
    if not message.catalog_id or not message.catalog_revision:
        raise MasterdataDecodeError("Masterdata record has no catalog provenance")
    if not message.HasField("published_at"):
        raise MasterdataDecodeError("Masterdata record has no publication timestamp")
    if message.WhichOneof("connection") != "c37_118_tcp":
        raise MasterdataDecodeError("Masterdata record has no C37.118 TCP connection")
    if not message.location.site_id or not message.location.display_name:
        raise MasterdataDecodeError("Masterdata record has incomplete source location")

    connection = message.c37_118_tcp
    _validate_connection(connection.ip_address, connection.port, connection.pmu_idcode)
    signals = _decode_signals(message)
    try:
        published_at = message.published_at.ToDatetime(tzinfo=timezone.utc)
    except ValueError as error:
        raise MasterdataDecodeError("Masterdata publication timestamp is invalid") from error
    return GatewaySource(
        source_id=source_id,
        catalog_id=message.catalog_id,
        catalog_revision=message.catalog_revision,
        published_at=published_at,
        site_id=message.location.site_id,
        display_name=message.location.display_name,
        ip_address=connection.ip_address,
        port=connection.port,
        pmu_idcode=connection.pmu_idcode,
        signals=signals,
    )


def decode_source_id(key: bytes | None) -> str:
    """Decode and validate a compacted-topic source key, including tombstones."""

    if not key:
        raise MasterdataDecodeError("Masterdata record has no Kafka key")
    try:
        source_id = key.decode("utf-8")
    except UnicodeDecodeError as error:
        raise MasterdataDecodeError("Masterdata Kafka key must be UTF-8") from error
    if not _IDENTIFIER.fullmatch(source_id):
        raise MasterdataDecodeError("Masterdata source_id has an unsupported format")
    return source_id


def _validate_connection(ip_address_text: str, port: int, pmu_idcode: int) -> None:
    try:
        ip_address(ip_address_text)
    except ValueError as error:
        raise MasterdataDecodeError("Masterdata C37.118 address is not a literal IP address") from error
    if not 1 <= port <= 65_535:
        raise MasterdataDecodeError("Masterdata C37.118 port is outside the accepted range")
    if not 1 <= pmu_idcode <= 65_535:
        raise MasterdataDecodeError("Masterdata C37.118 PMU IDCODE is outside the accepted range")


def _decode_signals(message: masterdata_pb2.SourceMasterdata) -> tuple[GatewaySignal, ...]:
    if not message.signals:
        raise MasterdataDecodeError("Masterdata record has no signal mappings")
    signal_ids: set[str] = set()
    mrids: set[str] = set()
    signals: list[GatewaySignal] = []
    for signal in message.signals:
        if not _IDENTIFIER.fullmatch(signal.signal_id):
            raise MasterdataDecodeError("Masterdata signal_id has an unsupported format")
        if not signal.source_channel or len(signal.source_channel) > 128:
            raise MasterdataDecodeError("Masterdata signal has an invalid source channel")
        if not _MRID.fullmatch(signal.mrid):
            raise MasterdataDecodeError("Masterdata signal has an invalid MRID")
        expected = _QUANTITY_RULES.get(signal.quantity)
        if expected is None or (signal.unit, signal.value_kind) != expected:
            raise MasterdataDecodeError("Masterdata signal has an unsupported quantity mapping")
        if signal.signal_id in signal_ids or signal.mrid in mrids:
            raise MasterdataDecodeError("Masterdata record repeats a signal ID or MRID")
        signal_ids.add(signal.signal_id)
        mrids.add(signal.mrid)
        signals.append(
            GatewaySignal(
                signal_id=signal.signal_id,
                source_channel=signal.source_channel,
                mrid=signal.mrid,
                quantity=signal.quantity,
                unit=signal.unit,
            )
        )
    return tuple(sorted(signals, key=lambda signal: signal.signal_id))