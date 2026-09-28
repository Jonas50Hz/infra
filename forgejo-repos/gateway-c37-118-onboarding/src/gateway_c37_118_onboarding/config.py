"""Strict parsing for Git-authored C37.118 PMU source catalog entries."""

from __future__ import annotations

from dataclasses import dataclass
from ipaddress import ip_address
from pathlib import Path
import re
from typing import Any

import yaml


IDENTIFIER_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
MRID_PATTERN = re.compile(r"^urn:[A-Za-z0-9][A-Za-z0-9:._-]{1,255}$")
VALUE_KINDS = frozenset({"double", "int", "uint", "bool", "string", "timestamp"})
QUANTITY_RULES = {
    "voltage": ("V", "double"),
    "current": ("A", "double"),
    "frequency": ("Hz", "double"),
    "rocof": ("Hz/s", "double"),
}
C37_118_V2_WIRE_VERSION = 2
C37_118_V2_SELECTOR_FIELDS = frozenset(
    {"phasor_magnitude_channel", "frequency", "rocof"}
)
C37_118_V2_PHASOR_QUANTITIES = frozenset({"voltage", "current"})
C37_118_V2_SINGLETON_CHANNELS = {"frequency": "FREQ", "rocof": "ROCOF"}


class CatalogError(ValueError):
    """Raised when the Git source catalog cannot safely become runtime state."""


@dataclass(frozen=True)
class C37_118V2SignalSelectorDefinition:
    """One explicit C37.118 v2 source-value selection rule."""

    kind: str
    phasor_magnitude_channel: str | None = None


@dataclass(frozen=True)
class SignalDefinition:
    """One C37.118 channel's stable Common Format mapping."""

    signal_id: str
    source_channel: str
    mrid: str
    value_kind: str
    quantity: str
    unit: str
    c37_118_v2_selector: C37_118V2SignalSelectorDefinition


@dataclass(frozen=True)
class SourceDefinition:
    """One PMU source that a later gateway may connect to."""

    source_id: str
    site_id: str
    display_name: str
    ip_address: str
    port: int
    pmu_idcode: int
    wire_version: int
    signals: tuple[SignalDefinition, ...]


@dataclass(frozen=True)
class Catalog:
    """Validated source records plus their Git provenance."""

    catalog_id: str
    catalog_revision: str
    sources: tuple[SourceDefinition, ...]


def load_catalog(
    directory: str | Path,
    catalog_id: str,
    catalog_revision: str,
) -> Catalog:
    """Load every source YAML file in deterministic source-ID order."""

    _identifier(catalog_id, "catalog_id")
    _revision(catalog_revision)
    catalog_directory = Path(directory)
    if not catalog_directory.is_dir():
        raise CatalogError(f"Catalog directory does not exist: {catalog_directory}")

    sources: list[SourceDefinition] = []
    source_ids: set[str] = set()
    mrids: set[str] = set()
    for source_path in sorted(catalog_directory.glob("*.yaml")):
        source = _load_source_file(source_path)
        if source.source_id in source_ids:
            raise CatalogError(f"Duplicate source_id {source.source_id!r}")
        source_ids.add(source.source_id)
        for signal in source.signals:
            if signal.mrid in mrids:
                raise CatalogError(f"Duplicate MRID {signal.mrid!r} across catalog sources")
            mrids.add(signal.mrid)
        sources.append(source)

    return Catalog(
        catalog_id=catalog_id,
        catalog_revision=catalog_revision,
        sources=tuple(sorted(sources, key=lambda source: source.source_id)),
    )


def _load_source_file(path: Path) -> SourceDefinition:
    try:
        with path.open(encoding="utf-8") as source_file:
            raw_source = yaml.safe_load(source_file)
    except OSError as error:
        raise CatalogError(f"Unable to read catalog source {path}: {error}") from error
    except yaml.YAMLError as error:
        raise CatalogError(f"Unable to parse catalog source {path}: {error}") from error

    location = str(path)
    source = _mapping(raw_source, location)
    _reject_unknown_keys(source, {"source_id", "location", "connection", "signals"}, location)
    source_id = _identifier(source.get("source_id"), f"{location}.source_id")
    if path.stem != source_id:
        raise CatalogError(f"{location} file name must match source_id {source_id!r}")

    site_id, display_name = _parse_location(source.get("location"), location)
    address, port, pmu_idcode, wire_version = _parse_connection(source.get("connection"), location)
    signals = _parse_signals(source.get("signals"), location)
    return SourceDefinition(
        source_id=source_id,
        site_id=site_id,
        display_name=display_name,
        ip_address=address,
        port=port,
        pmu_idcode=pmu_idcode,
        wire_version=wire_version,
        signals=signals,
    )


def _parse_location(raw_location: Any, source_location: str) -> tuple[str, str]:
    location = _mapping(raw_location, f"{source_location}.location")
    _reject_unknown_keys(location, {"site_id", "display_name"}, f"{source_location}.location")
    return (
        _identifier(location.get("site_id"), f"{source_location}.location.site_id"),
        _text(location.get("display_name"), f"{source_location}.location.display_name", 128),
    )


def _parse_connection(raw_connection: Any, source_location: str) -> tuple[str, int, int, int]:
    location = f"{source_location}.connection"
    connection = _mapping(raw_connection, location)
    _reject_unknown_keys(
        connection,
        {"protocol", "ip_address", "port", "pmu_idcode", "wire_version"},
        location,
    )
    protocol = connection.get("protocol")
    if protocol != "c37_118_tcp":
        raise CatalogError(f"{location}.protocol must be 'c37_118_tcp'")

    address = _text(connection.get("ip_address"), f"{location}.ip_address", 45)
    try:
        ip_address(address)
    except ValueError as error:
        raise CatalogError(f"{location}.ip_address must be a literal IPv4 or IPv6 address") from error

    wire_version = _bounded_integer(
        connection.get("wire_version"),
        f"{location}.wire_version",
        1,
        15,
    )
    if wire_version != C37_118_V2_WIRE_VERSION:
        raise CatalogError(f"{location}.wire_version must be {C37_118_V2_WIRE_VERSION}")

    return (
        address,
        _bounded_integer(connection.get("port"), f"{location}.port", 1, 65_535),
        _bounded_integer(
            connection.get("pmu_idcode"),
            f"{location}.pmu_idcode",
            1,
            65_535,
        ),
        wire_version,
    )


def _parse_signals(raw_signals: Any, source_location: str) -> tuple[SignalDefinition, ...]:
    location = f"{source_location}.signals"
    if not isinstance(raw_signals, list) or not raw_signals:
        raise CatalogError(f"{location} must be a non-empty list")

    signal_ids: set[str] = set()
    source_channels: set[str] = set()
    selector_identities: set[tuple[str, str]] = set()
    mrids: set[str] = set()
    parsed: list[SignalDefinition] = []
    for index, raw_signal in enumerate(raw_signals):
        signal_location = f"{location}[{index}]"
        signal = _mapping(raw_signal, signal_location)
        _reject_unknown_keys(
            signal,
            {
                "signal_id",
                "source_channel",
                "mrid",
                "value_kind",
                "quantity",
                "unit",
                "c37_118_v2_selector",
            },
            signal_location,
        )
        signal_id = _identifier(signal.get("signal_id"), f"{signal_location}.signal_id")
        source_channel = _c37_118_v2_channel_name(
            signal.get("source_channel"),
            f"{signal_location}.source_channel",
        )
        mrid = _mrid(signal.get("mrid"), f"{signal_location}.mrid")
        value_kind = _value_kind(signal.get("value_kind"), f"{signal_location}.value_kind")
        quantity = _text(signal.get("quantity"), f"{signal_location}.quantity", 64)
        unit = _text(signal.get("unit"), f"{signal_location}.unit", 32)
        _validate_quantity_mapping(quantity, unit, value_kind, signal_location)
        selector = _parse_c37_118_v2_selector(
            signal.get("c37_118_v2_selector"),
            source_channel,
            quantity,
            signal_location,
        )
        selector_identity = (selector.kind, selector.phasor_magnitude_channel or "")
        if signal_id in signal_ids:
            raise CatalogError(f"Duplicate signal_id {signal_id!r} in {source_location}")
        if source_channel in source_channels:
            raise CatalogError(f"Duplicate source_channel {source_channel!r} in {source_location}")
        if selector_identity in selector_identities:
            raise CatalogError(f"Duplicate C37.118 v2 selector in {source_location}")
        if mrid in mrids:
            raise CatalogError(f"Duplicate MRID {mrid!r} in {source_location}")
        signal_ids.add(signal_id)
        source_channels.add(source_channel)
        selector_identities.add(selector_identity)
        mrids.add(mrid)
        parsed.append(
            SignalDefinition(
                signal_id=signal_id,
                source_channel=source_channel,
                mrid=mrid,
                value_kind=value_kind,
                quantity=quantity,
                unit=unit,
                c37_118_v2_selector=selector,
            )
        )
    return tuple(sorted(parsed, key=lambda signal: signal.signal_id))


def _validate_quantity_mapping(
    quantity: str,
    unit: str,
    value_kind: str,
    location: str,
) -> None:
    rule = QUANTITY_RULES.get(quantity)
    if rule is None:
        supported = ", ".join(sorted(QUANTITY_RULES))
        raise CatalogError(f"{location}.quantity must be one of: {supported}")
    expected_unit, expected_value_kind = rule
    if unit != expected_unit or value_kind != expected_value_kind:
        raise CatalogError(
            f"{location} requires unit {expected_unit!r} and value_kind "
            f"{expected_value_kind!r} for quantity {quantity!r}"
        )


def _parse_c37_118_v2_selector(
    raw_selector: Any,
    source_channel: str,
    quantity: str,
    signal_location: str,
) -> C37_118V2SignalSelectorDefinition:
    location = f"{signal_location}.c37_118_v2_selector"
    selector = _mapping(raw_selector, location)
    _reject_unknown_keys(selector, C37_118_V2_SELECTOR_FIELDS, location)
    if len(selector) != 1:
        raise CatalogError(f"{location} must contain exactly one selector")

    selector_kind, selector_value = next(iter(selector.items()))
    if selector_kind == "phasor_magnitude_channel":
        channel = _c37_118_v2_channel_name(selector_value, f"{location}.{selector_kind}")
        if quantity not in C37_118_V2_PHASOR_QUANTITIES:
            raise CatalogError(f"{location}.{selector_kind} requires voltage or current quantity")
        if source_channel != channel:
            raise CatalogError(f"{location}.{selector_kind} must equal source_channel")
        return C37_118V2SignalSelectorDefinition(
            kind="phasor_magnitude",
            phasor_magnitude_channel=channel,
        )

    if selector_value is not True:
        raise CatalogError(f"{location}.{selector_kind} must be true")
    if quantity != selector_kind:
        raise CatalogError(f"{location}.{selector_kind} requires {selector_kind} quantity")
    expected_channel = C37_118_V2_SINGLETON_CHANNELS[selector_kind]
    if source_channel != expected_channel:
        raise CatalogError(f"{location}.{selector_kind} requires source_channel {expected_channel!r}")
    return C37_118V2SignalSelectorDefinition(kind=selector_kind)


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise CatalogError(f"{location} must be a mapping")
    return value


def _reject_unknown_keys(mapping: dict[str, Any], allowed: set[str], location: str) -> None:
    unknown = set(mapping).difference(allowed)
    if unknown:
        names = ", ".join(sorted(unknown))
        raise CatalogError(f"{location} contains unsupported key(s): {names}")


def _identifier(value: Any, location: str) -> str:
    text = _text(value, location, 63)
    if not IDENTIFIER_PATTERN.fullmatch(text):
        raise CatalogError(
            f"{location} must use lowercase letters, numbers, and hyphens, "
            "starting with a letter or number"
        )
    return text


def _revision(value: str) -> None:
    if not isinstance(value, str) or not value or len(value) > 128:
        raise CatalogError("catalog_revision must be a non-empty string of at most 128 characters")
    if not re.fullmatch(r"[A-Za-z0-9._-]+", value):
        raise CatalogError("catalog_revision contains unsupported characters")


def _mrid(value: Any, location: str) -> str:
    text = _text(value, location, 256)
    if not MRID_PATTERN.fullmatch(text):
        raise CatalogError(f"{location} must be a URN-style MRID")
    return text


def _value_kind(value: Any, location: str) -> str:
    if not isinstance(value, str) or value not in VALUE_KINDS:
        names = ", ".join(sorted(VALUE_KINDS))
        raise CatalogError(f"{location} must be one of: {names}")
    return value


def _text(value: Any, location: str, maximum_length: int) -> str:
    if not isinstance(value, str) or not value.strip() or len(value.strip()) > maximum_length:
        raise CatalogError(f"{location} must be a non-empty string of at most {maximum_length} characters")
    return value.strip()


def _c37_118_v2_channel_name(value: Any, location: str) -> str:
    text = _text(value, location, 16)
    if not text.isascii():
        raise CatalogError(f"{location} must be ASCII")
    return text


def _bounded_integer(value: Any, location: str, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise CatalogError(f"{location} must be an integer between {minimum} and {maximum}")
    return value