"""Strict, reviewed C37.118 v2 source catalog loading."""

from __future__ import annotations

from dataclasses import dataclass
import ipaddress
from pathlib import Path
import re
from typing import Any, Mapping, Union

import yaml


class CatalogError(ValueError):
    """Raised when the reviewed enrollment catalog is not safe to publish."""


_SOURCE_ID = re.compile(r"^[a-z][a-z0-9-]{1,62}$")
_CATALOG_KEYS = frozenset({"catalog_id"})
_SOURCE_KEYS = frozenset({"source_id", "location", "connection", "signals"})
_LOCATION_KEYS = frozenset({"site_id", "display_name"})
_CONNECTION_KEYS = frozenset({"ip_address", "port", "pmu_idcode", "wire_version"})
_SIGNAL_KEYS = frozenset(
    {"signal_id", "source_channel", "mrid", "value_kind", "quantity", "unit", "selector"}
)
_SELECTOR_KEYS = frozenset({"phasor_magnitude_channel", "frequency", "rocof"})


@dataclass(frozen=True)
class Signal:
    """A stable source channel to Common Format signal mapping."""

    signal_id: str
    source_channel: str
    mrid: str
    quantity: str
    unit: str
    selector: str


@dataclass(frozen=True)
class Source:
    """One approved legacy-v2 C37.118 TCP source."""

    source_id: str
    site_id: str
    display_name: str
    ip_address: str
    port: int
    pmu_idcode: int
    signals: tuple[Signal, ...]


@dataclass(frozen=True)
class Catalog:
    """The complete Git-authoritative source catalog."""

    catalog_id: str
    sources: tuple[Source, ...]

    @property
    def mrids(self) -> frozenset[str]:
        return frozenset(signal.mrid for source in self.sources for signal in source.signals)

    def source(self, source_id: str) -> Source:
        for source in self.sources:
            if source.source_id == source_id:
                return source
        raise CatalogError(f"Unknown catalog source: {source_id}")


def load_catalog(directory: Union[str, Path]) -> Catalog:
    """Load catalog files, rejecting unreviewed shape and unsafe identities."""

    root = Path(directory)
    catalog_raw = _yaml_mapping(root / "catalog.yaml")
    _reject_unknown(catalog_raw, _CATALOG_KEYS, "catalog")
    catalog_id = _required_string(catalog_raw, "catalog_id", "catalog")
    source_directory = root / "sources"
    if not source_directory.is_dir():
        raise CatalogError("catalog.sources directory is required")
    paths = sorted(source_directory.glob("*.yaml"))
    if not paths:
        raise CatalogError("catalog.sources must contain at least one source")

    sources = tuple(_source_from_yaml(path) for path in paths)
    source_ids = {source.source_id for source in sources}
    if len(source_ids) != len(sources):
        raise CatalogError("catalog contains duplicate source_id")
    mrids: set[str] = set()
    for source in sources:
        for signal in source.signals:
            if signal.mrid in mrids:
                raise CatalogError(f"catalog MRID is not unique: {signal.mrid}")
            mrids.add(signal.mrid)
    return Catalog(catalog_id=catalog_id, sources=tuple(sorted(sources, key=lambda item: item.source_id)))


def _source_from_yaml(path: Path) -> Source:
    raw = _yaml_mapping(path)
    _reject_unknown(raw, _SOURCE_KEYS, path.name)
    source_id = _required_identifier(raw, "source_id", path.name)
    if path.stem != source_id:
        raise CatalogError(f"{path.name} must match source_id {source_id}")
    location = _mapping(raw.get("location"), f"{path.name}.location")
    _reject_unknown(location, _LOCATION_KEYS, f"{path.name}.location")
    connection = _mapping(raw.get("connection"), f"{path.name}.connection")
    _reject_unknown(connection, _CONNECTION_KEYS, f"{path.name}.connection")
    ip_address = _required_string(connection, "ip_address", f"{path.name}.connection")
    try:
        ipaddress.ip_address(ip_address)
    except ValueError as error:
        raise CatalogError(f"{path.name}.connection.ip_address must be a literal IP address") from error
    if _integer(connection, "wire_version", f"{path.name}.connection", 2, 2) != 2:
        raise CatalogError(f"{path.name}.connection.wire_version must be 2")
    raw_signals = raw.get("signals")
    if not isinstance(raw_signals, list) or not raw_signals:
        raise CatalogError(f"{path.name}.signals must be a non-empty list")
    signals = tuple(_signal(value, f"{path.name}.signals[{index}]") for index, value in enumerate(raw_signals))
    if len({item.signal_id for item in signals}) != len(signals):
        raise CatalogError(f"{path.name}.signals has duplicate signal_id")
    if len({item.source_channel for item in signals}) != len(signals):
        raise CatalogError(f"{path.name}.signals has duplicate source_channel")
    if len({item.selector for item in signals}) != len(signals):
        raise CatalogError(f"{path.name}.signals has duplicate selector")
    return Source(
        source_id=source_id,
        site_id=_required_identifier(location, "site_id", f"{path.name}.location"),
        display_name=_required_string(location, "display_name", f"{path.name}.location"),
        ip_address=ip_address,
        port=_integer(connection, "port", f"{path.name}.connection", 1, 65_535),
        pmu_idcode=_integer(connection, "pmu_idcode", f"{path.name}.connection", 1, 65_535),
        signals=signals,
    )


def _signal(raw_signal: Any, location: str) -> Signal:
    raw = _mapping(raw_signal, location)
    _reject_unknown(raw, _SIGNAL_KEYS, location)
    if raw.get("value_kind") != "double":
        raise CatalogError(f"{location}.value_kind must be double")
    signal_id = _required_identifier(raw, "signal_id", location)
    source_channel = _required_string(raw, "source_channel", location)
    mrid = _required_string(raw, "mrid", location)
    selector_raw = _mapping(raw.get("selector"), f"{location}.selector")
    _reject_unknown(selector_raw, _SELECTOR_KEYS, f"{location}.selector")
    selected = [key for key, value in selector_raw.items() if value is not None]
    if len(selected) != 1:
        raise CatalogError(f"{location}.selector must have exactly one selector")
    selector_name = selected[0]
    value = selector_raw[selector_name]
    if selector_name == "phasor_magnitude_channel":
        if not isinstance(value, str) or value != source_channel:
            raise CatalogError(f"{location}.selector phasor channel must equal source_channel")
        quantity_unit = {"voltage": "V", "current": "A"}
        if raw.get("quantity") not in quantity_unit or raw.get("unit") != quantity_unit[raw["quantity"]]:
            raise CatalogError(f"{location} phasor must be voltage/V or current/A")
        selector = f"phasor:{value}"
    elif selector_name == "frequency":
        if value is not True or source_channel != "FREQ" or raw.get("quantity") != "frequency" or raw.get("unit") != "Hz":
            raise CatalogError(f"{location} frequency selector must use FREQ, frequency, and Hz")
        selector = "frequency"
    else:
        if value is not True or source_channel != "DFREQ" or raw.get("quantity") != "rocof" or raw.get("unit") != "Hz/s":
            raise CatalogError(f"{location} rocof selector must use DFREQ, rocof, and Hz/s")
        selector = "rocof"
    return Signal(signal_id, source_channel, mrid, str(raw["quantity"]), str(raw["unit"]), selector)


def _yaml_mapping(path: Path) -> Mapping[str, Any]:
    try:
        loaded = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as error:
        raise CatalogError(f"Unable to read {path}: {error}") from error
    return _mapping(loaded, str(path))


def _mapping(value: Any, location: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CatalogError(f"{location} must be a mapping")
    return value


def _reject_unknown(value: Mapping[str, Any], allowed: frozenset[str], location: str) -> None:
    unknown = set(value).difference(allowed)
    if unknown:
        raise CatalogError(f"{location} has unsupported field(s): {', '.join(sorted(unknown))}")


def _required_string(value: Mapping[str, Any], name: str, location: str) -> str:
    item = value.get(name)
    if not isinstance(item, str) or not item.strip():
        raise CatalogError(f"{location}.{name} must be a non-empty string")
    return item.strip()


def _required_identifier(value: Mapping[str, Any], name: str, location: str) -> str:
    item = _required_string(value, name, location)
    if not _SOURCE_ID.fullmatch(item):
        raise CatalogError(f"{location}.{name} must be a safe stable identifier")
    return item


def _integer(value: Mapping[str, Any], name: str, location: str, minimum: int, maximum: int) -> int:
    item = value.get(name)
    if isinstance(item, bool) or not isinstance(item, int) or not minimum <= item <= maximum:
        raise CatalogError(f"{location}.{name} must be an integer between {minimum} and {maximum}")
    return item
