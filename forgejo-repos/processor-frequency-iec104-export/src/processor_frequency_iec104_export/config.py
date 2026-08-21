"""Validated settings for direct frequency-to-IEC 104 export."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os
from pathlib import Path
from types import MappingProxyType
from typing import Any

import yaml


class ConfigurationError(ValueError):
    """Raised when direct frequency export settings are incomplete or unsafe."""


_MONITOR_CAUSE_CODES = frozenset({1, 2, 3, 4, 5, *range(20, 37)})
_LEGACY_MAPPING_SETTINGS = (
    "FREQUENCY_IEC104_CAUSE_CODE",
    "FREQUENCY_IEC104_COMMON_ADDRESS",
    "FREQUENCY_IEC104_INFORMATION_OBJECT_ADDRESS",
    "FREQUENCY_IEC104_SOURCE_MRID",
)


@dataclass(frozen=True)
class ExportMapping:
    """One reviewed frequency MRID to outbound IEC 104 point mapping."""

    common_address: int
    information_object_address: int
    cause_code: int


@dataclass(frozen=True)
class Settings:
    """Kafka settings and a reviewed MRID-to-IEC mapping."""

    config_path: str
    consumer_group: str
    input_topic: str
    kafka_bootstrap_servers: str
    mappings: Mapping[str, ExportMapping]
    output_topic: str

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> "Settings":
        """Load runtime settings and the reviewed frequency export map."""

        values = os.environ if environment is None else environment
        _reject_legacy_mapping_settings(values)
        config_path = _absolute_path(
            values,
            "FREQUENCY_IEC104_CONFIG_PATH",
            "/etc/wama/frequency-iec104-export.yaml",
        )
        return cls(
            config_path=config_path,
            consumer_group=_required(
                values,
                "WAMA_PROCESSOR_CONSUMER_GROUP",
                "processor-frequency-iec104-export",
            ),
            input_topic=_required(values, "WAMA_INPUT_TOPIC", "LiveMeasurement"),
            kafka_bootstrap_servers=_required(
                values,
                "WAMA_KAFKA_BOOTSTRAP_SERVERS",
                "kafka:9092",
            ),
            mappings=load_export_mappings(config_path),
            output_topic=_required(values, "WAMA_OUTPUT_TOPIC", "Export"),
        )

    def mapping_for(self, mrid: str) -> ExportMapping | None:
        """Return the reviewed IEC mapping for an incoming frequency MRID."""

        return self.mappings.get(mrid)


def load_export_mappings(path: str | Path) -> Mapping[str, ExportMapping]:
    """Load one explicit, versioned map of Common Format MRIDs to IEC points."""

    config_path = Path(path)
    try:
        with config_path.open(encoding="utf-8") as config_file:
            raw_config = yaml.safe_load(config_file)
    except OSError as error:
        raise ConfigurationError(f"Unable to read frequency export configuration {config_path}: {error}") from error
    except yaml.YAMLError as error:
        raise ConfigurationError(f"Unable to parse frequency export configuration {config_path}: {error}") from error

    config = _mapping(raw_config, "configuration")
    _reject_unknown_keys(config, {"version", "exports"}, "configuration")
    version = config.get("version")
    if isinstance(version, bool) or version != 1:
        raise ConfigurationError("configuration.version must be 1")
    raw_exports = config.get("exports")
    if not isinstance(raw_exports, list) or not raw_exports:
        raise ConfigurationError("configuration.exports must be a non-empty list")

    mappings: dict[str, ExportMapping] = {}
    points: set[tuple[int, int]] = set()
    for index, raw_export in enumerate(raw_exports):
        location = f"configuration.exports[{index}]"
        raw_mapping = _mapping(raw_export, location)
        _reject_unknown_keys(
            raw_mapping,
            {"mrid", "common_address", "information_object_address", "cause_code"},
            location,
        )
        mrid = _required_string(raw_mapping, "mrid", location)
        mapping = ExportMapping(
            common_address=_integer(raw_mapping, "common_address", location, 1, 65_535),
            information_object_address=_integer(
                raw_mapping,
                "information_object_address",
                location,
                1,
                16_777_215,
            ),
            cause_code=_integer(raw_mapping, "cause_code", location, 1, 63),
        )
        if mapping.cause_code not in _MONITOR_CAUSE_CODES:
            raise ConfigurationError(f"{location}.cause_code must be monitor-direction compatible")
        if mrid in mappings:
            raise ConfigurationError(f"{location}.mrid duplicates an existing mapping")
        point = (mapping.common_address, mapping.information_object_address)
        if point in points:
            raise ConfigurationError(
                f"{location} duplicates IEC point CA {point[0]} / IOA {point[1]}"
            )
        mappings[mrid] = mapping
        points.add(point)
    return MappingProxyType(mappings)


def _required(values: Mapping[str, str], name: str, default: str) -> str:
    value = values.get(name, default).strip()
    if not value:
        raise ConfigurationError(f"{name} must not be empty")
    return value


def _absolute_path(values: Mapping[str, str], name: str, default: str) -> str:
    value = _required(values, name, default)
    if not Path(value).is_absolute():
        raise ConfigurationError(f"{name} must be an absolute path")
    return value


def _integer(
    values: Mapping[str, Any],
    name: str,
    location: str,
    minimum: int,
    maximum: int,
) -> int:
    value = values.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigurationError(f"{location}.{name} must be an integer")
    if not minimum <= value <= maximum:
        raise ConfigurationError(f"{location}.{name} must be between {minimum} and {maximum}")
    return value


def _mapping(value: Any, location: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ConfigurationError(f"{location} must be a mapping")
    return value


def _reject_unknown_keys(mapping: Mapping[str, Any], allowed_keys: set[str], location: str) -> None:
    unknown_keys = set(mapping).difference(allowed_keys)
    if unknown_keys:
        names = ", ".join(sorted(str(key) for key in unknown_keys))
        raise ConfigurationError(f"{location} has unsupported field(s): {names}")


def _required_string(mapping: Mapping[str, Any], name: str, location: str) -> str:
    value = mapping.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(f"{location}.{name} must be a non-empty string")
    return value.strip()


def _reject_legacy_mapping_settings(values: Mapping[str, str]) -> None:
    configured = [name for name in _LEGACY_MAPPING_SETTINGS if name in values]
    if configured:
        names = ", ".join(configured)
        raise ConfigurationError(
            f"Legacy single-mapping setting(s) are not supported: {names}; "
            "use FREQUENCY_IEC104_CONFIG_PATH"
        )