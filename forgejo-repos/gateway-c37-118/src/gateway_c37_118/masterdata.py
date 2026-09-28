"""Raw-Protobuf Masterdata encoding and compacted-topic reconciliation."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
import ipaddress
import re
from typing import Optional, Protocol, Union

from google.protobuf.timestamp_pb2 import Timestamp

from gateway_c37_118.catalog import Catalog, Source
from gateway_c37_118.generated import masterdata_pb2


class MasterdataError(ValueError):
    """Raised when compacted Masterdata ownership is unsafe."""


class Delivery(Protocol):
    def get(self, timeout: Optional[float] = None) -> object: ...


class Producer(Protocol):
    def send(self, topic: str, *, key: bytes, value: Optional[bytes]) -> Delivery: ...


_IDENTIFIER = re.compile(r"^[A-Za-z0-9._:-]+$")


def encode_source(source: Source, catalog_id: str, revision: str, published_at: datetime) -> bytes:
    """Encode one source deterministically; only publication time is intentionally dynamic."""

    record = masterdata_pb2.SourceMasterdata(
        source_id=source.source_id,
        catalog_id=catalog_id,
        catalog_revision=revision,
        location=masterdata_pb2.SiteLocation(site_id=source.site_id, display_name=source.display_name),
        c37_118_tcp=masterdata_pb2.C37_118TcpConnection(
            ip_address=source.ip_address,
            port=source.port,
            pmu_idcode=source.pmu_idcode,
            wire_version=masterdata_pb2.C37_118_WIRE_VERSION_2,
        ),
    )
    timestamp = Timestamp()
    timestamp.FromDatetime(published_at.astimezone(timezone.utc))
    record.published_at.CopyFrom(timestamp)
    for signal in source.signals:
        target = record.signals.add(
            signal_id=signal.signal_id,
            source_channel=signal.source_channel,
            mrid=signal.mrid,
            value_kind=masterdata_pb2.MCCS_VALUE_KIND_DOUBLE,
            quantity=signal.quantity,
            unit=signal.unit,
        )
        if signal.selector.startswith("phasor:"):
            target.c37_118_v2_selector.phasor_magnitude_channel = signal.selector.removeprefix("phasor:")
        elif signal.selector == "frequency":
            target.c37_118_v2_selector.frequency = True
        else:
            target.c37_118_v2_selector.rocof = True
    return record.SerializeToString(deterministic=True)


def reconcile(
    producer: Producer,
    topic: str,
    catalog: Catalog,
    revision: str,
    existing: Mapping[str, bytes],
    published_at: datetime,
) -> list[str]:
    """Validate existing records then publish ordered values and owned tombstones."""

    previous = _validate_existing(existing, catalog)
    changed: list[str] = []
    for source in catalog.sources:
        producer.send(
            topic,
            key=source.source_id.encode("utf-8"),
            value=encode_source(source, catalog.catalog_id, revision, published_at),
        ).get(timeout=10)
        changed.append(source.source_id)
    for source_id in sorted(previous.difference({source.source_id for source in catalog.sources})):
        producer.send(topic, key=source_id.encode("utf-8"), value=None).get(timeout=10)
        changed.append(source_id)
    return changed


def verify_catalog_projection(
    existing: Mapping[str, bytes], catalog: Catalog, revision: str
) -> frozenset[str]:
    """Validate that every approved source is the active compacted projection."""

    _validate_existing(existing, catalog)
    source_ids = frozenset(source.source_id for source in catalog.sources)
    missing = sorted(source_ids.difference(existing))
    if missing:
        raise MasterdataError(f"Masterdata is missing catalog sources: {', '.join(missing)}")
    for source in catalog.sources:
        record = masterdata_pb2.SourceMasterdata()
        record.ParseFromString(existing[source.source_id])
        if record.catalog_id != catalog.catalog_id or record.catalog_revision != revision:
            raise MasterdataError(f"Masterdata catalog metadata does not match {source.source_id}")
        if (
            record.location.site_id != source.site_id
            or record.location.display_name != source.display_name
            or record.c37_118_tcp.ip_address != source.ip_address
            or record.c37_118_tcp.port != source.port
            or record.c37_118_tcp.pmu_idcode != source.pmu_idcode
        ):
            raise MasterdataError(f"Masterdata connection does not match {source.source_id}")
        expected_signals = [
            (signal.signal_id, signal.source_channel, signal.mrid, signal.quantity, signal.unit, signal.selector)
            for signal in source.signals
        ]
        actual_signals = [
            _signal_values(signal)
            for signal in record.signals
        ]
        if actual_signals != expected_signals:
            raise MasterdataError(f"Masterdata signals do not match {source.source_id}")
    return source_ids


def _validate_existing(existing: Mapping[str, bytes], catalog: Catalog) -> set[str]:
    owned: set[str] = set()
    mrid_owner: dict[str, tuple[str, str]] = {}
    desired = {
        (source.source_id, signal.signal_id): signal.mrid
        for source in catalog.sources
        for signal in source.signals
    }
    desired_mrid_owner = {
        signal.mrid: (source.source_id, signal.signal_id)
        for source in catalog.sources
        for signal in source.signals
    }
    for key, raw in existing.items():
        record = masterdata_pb2.SourceMasterdata()
        try:
            record.ParseFromString(raw)
        except Exception as error:
            raise MasterdataError(f"Malformed existing Masterdata record for {key}") from error
        if key != record.source_id:
            raise MasterdataError(f"Masterdata Kafka key does not match payload source_id: {key}")
        _validate_existing_record(record, key)
        if record.catalog_id == catalog.catalog_id:
            owned.add(key)
            for signal in record.signals:
                expected = desired.get((key, signal.signal_id))
                if expected is not None and expected != signal.mrid:
                    raise MasterdataError(f"MRID changed for {key}/{signal.signal_id}")
        for signal in record.signals:
            owner = (record.source_id, signal.signal_id)
            desired_owner = desired_mrid_owner.get(signal.mrid)
            if desired_owner is not None and desired_owner != owner:
                raise MasterdataError(f"MRID ownership collision for {signal.mrid}")
            previous = mrid_owner.setdefault(signal.mrid, owner)
            if previous != owner:
                raise MasterdataError(f"MRID ownership collision for {signal.mrid}")
    return owned


def _signal_values(signal: masterdata_pb2.Signal) -> tuple[str, str, str, str, str, str]:
    selector_name = signal.c37_118_v2_selector.WhichOneof("selector")
    if selector_name == "phasor_magnitude_channel":
        selector = f"phasor:{signal.c37_118_v2_selector.phasor_magnitude_channel}"
    else:
        selector = selector_name or ""
    return (
        signal.signal_id,
        signal.source_channel,
        signal.mrid,
        signal.quantity,
        signal.unit,
        selector,
    )


def _validate_existing_record(record: masterdata_pb2.SourceMasterdata, key: str) -> None:
    """Reject incomplete retained state before using it for ownership decisions."""

    if (
        not _IDENTIFIER.fullmatch(record.source_id)
        or not record.catalog_id
        or not record.catalog_revision
        or not record.HasField("published_at")
        or not record.HasField("location")
        or not _IDENTIFIER.fullmatch(record.location.site_id)
        or not record.location.display_name.strip()
        or record.WhichOneof("connection") != "c37_118_tcp"
        or not record.signals
    ):
        raise MasterdataError(f"Malformed existing Masterdata record for {key}")
    try:
        record.published_at.ToDatetime(tzinfo=timezone.utc)
    except ValueError as error:
        raise MasterdataError(f"Malformed existing Masterdata record for {key}") from error
    connection = record.c37_118_tcp
    try:
        ipaddress.ip_address(connection.ip_address)
    except ValueError as error:
        raise MasterdataError(f"Malformed existing Masterdata record for {key}") from error
    if (
        not 1 <= connection.port <= 65_535
        or not 1 <= connection.pmu_idcode <= 65_535
        or connection.wire_version != masterdata_pb2.C37_118_WIRE_VERSION_2
    ):
        raise MasterdataError(f"Malformed existing Masterdata record for {key}")
    seen_signal_ids: set[str] = set()
    seen_channels: set[str] = set()
    seen_selectors: set[tuple[str, Union[str, bool]]] = set()
    for signal in record.signals:
        selector_name = signal.c37_118_v2_selector.WhichOneof("selector")
        if (
            not _IDENTIFIER.fullmatch(signal.signal_id)
            or not signal.source_channel.strip()
            or not signal.mrid.strip()
            or signal.value_kind != masterdata_pb2.MCCS_VALUE_KIND_DOUBLE
            or not signal.quantity.strip()
            or not signal.unit.strip()
            or selector_name is None
        ):
            raise MasterdataError(f"Malformed existing Masterdata record for {key}")
        if selector_name == "phasor_magnitude_channel":
            selector_value: Union[str, bool] = signal.c37_118_v2_selector.phasor_magnitude_channel
            valid_selector = (
                selector_value == signal.source_channel
                and (signal.quantity, signal.unit) in {("voltage", "V"), ("current", "A")}
            )
        elif selector_name == "frequency":
            selector_value = signal.c37_118_v2_selector.frequency
            valid_selector = selector_value is True and (
                signal.source_channel,
                signal.quantity,
                signal.unit,
            ) == ("FREQ", "frequency", "Hz")
        else:
            selector_value = signal.c37_118_v2_selector.rocof
            valid_selector = selector_value is True and (
                signal.source_channel,
                signal.quantity,
                signal.unit,
            ) == ("DFREQ", "rocof", "Hz/s")
        if (
            not valid_selector
            or signal.signal_id in seen_signal_ids
            or signal.source_channel in seen_channels
            or (selector_name, selector_value) in seen_selectors
        ):
            raise MasterdataError(f"Malformed existing Masterdata record for {key}")
        seen_signal_ids.add(signal.signal_id)
        seen_channels.add(signal.source_channel)
        seen_selectors.add((selector_name, selector_value))
