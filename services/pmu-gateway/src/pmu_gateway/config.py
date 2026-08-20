"""Startup configuration parsing for the fake PMU gateway."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import math
from pathlib import Path
from typing import Any

import yaml

DEFAULT_PUBLISH_INTERVAL_MS = 1_000
VALUE_FIELDS = frozenset(
    {
        "double_value",
        "int_value",
        "uint_value",
        "bool_value",
        "string_value",
        "timestamp_value",
    }
)
QUALITY_FIELDS = frozenset(
    {"valid", "substituted", "operator_blocked", "overflow", "old_data"}
)


class ConfigurationError(ValueError):
    """Raised when the configured startup fixture is not valid."""


@dataclass(frozen=True)
class MessageDefinition:
    """One configured Common Format measurement to publish."""

    mrid: str
    value_field: str
    value: bool | datetime | float | int | str
    quality: dict[str, bool]
    field_timestamp_offset_ms: int | None
    value_jitter: float = 0.0


@dataclass(frozen=True)
class GatewayConfig:
    """Validated fake-gateway configuration loaded at process startup."""

    publish_interval_ms: int
    messages: tuple[MessageDefinition, ...]


def load_config(path: str | Path, interval_override: str | None = None) -> GatewayConfig:
    """Load and validate the fixture once before connecting to Kafka."""

    config_path = Path(path)
    try:
        with config_path.open(encoding="utf-8") as config_file:
            raw_config = yaml.safe_load(config_file)
    except OSError as error:
        raise ConfigurationError(
            f"Unable to read PMU gateway config {config_path}: {error}"
        ) from error
    except yaml.YAMLError as error:
        raise ConfigurationError(
            f"Unable to parse PMU gateway config {config_path}: {error}"
        ) from error

    config = _mapping(raw_config, "configuration")
    _reject_unknown_keys(config, {"publish_interval_ms", "messages"}, "configuration")

    interval = _parse_positive_int(
        config.get("publish_interval_ms", DEFAULT_PUBLISH_INTERVAL_MS),
        "publish_interval_ms",
    )
    if interval_override is not None and interval_override.strip():
        interval = _parse_environment_interval(
            interval_override,
        )

    raw_messages = config.get("messages")
    if not isinstance(raw_messages, list) or not raw_messages:
        raise ConfigurationError("configuration.messages must be a non-empty list")

    return GatewayConfig(
        publish_interval_ms=interval,
        messages=tuple(
            _parse_message(raw_message, index)
            for index, raw_message in enumerate(raw_messages)
        ),
    )


def _parse_message(raw_message: Any, index: int) -> MessageDefinition:
    location = f"configuration.messages[{index}]"
    message = _mapping(raw_message, location)
    _reject_unknown_keys(
        message,
        {"mrid", "value", "quality", "field_timestamp_offset_ms", "value_jitter"},
        location,
    )

    raw_mrid = message.get("mrid")
    if not isinstance(raw_mrid, str) or not raw_mrid.strip():
        raise ConfigurationError(f"{location}.mrid must be a non-empty string")

    value_field, value = _parse_value(message.get("value"), location)
    quality = _parse_quality(message.get("quality"), location)

    offset: int | None = None
    if "field_timestamp_offset_ms" in message:
        offset = _parse_non_negative_int(
            message["field_timestamp_offset_ms"],
            f"{location}.field_timestamp_offset_ms",
        )

    value_jitter = 0.0
    if "value_jitter" in message:
        if value_field != "double_value":
            raise ConfigurationError(
                f"{location}.value_jitter is only supported for double_value"
            )
        value_jitter = _parse_non_negative_finite_number(
            message["value_jitter"],
            f"{location}.value_jitter",
        )

    return MessageDefinition(
        mrid=raw_mrid.strip(),
        value_field=value_field,
        value=value,
        quality=quality,
        field_timestamp_offset_ms=offset,
        value_jitter=value_jitter,
    )


def _parse_value(raw_value: Any, location: str) -> tuple[str, bool | datetime | float | int | str]:
    value = _mapping(raw_value, f"{location}.value")
    if len(value) != 1:
        raise ConfigurationError(
            f"{location}.value must contain exactly one Common Format value field"
        )

    value_field, raw_scalar = next(iter(value.items()))
    if value_field not in VALUE_FIELDS:
        supported_values = ", ".join(sorted(VALUE_FIELDS))
        raise ConfigurationError(
            f"{location}.value.{value_field} is unsupported; use one of: {supported_values}"
        )

    if value_field == "double_value":
        return value_field, _parse_number(raw_scalar, f"{location}.value.{value_field}")
    if value_field == "int_value":
        return value_field, _parse_int64(raw_scalar, f"{location}.value.{value_field}")
    if value_field == "uint_value":
        return value_field, _parse_uint32(raw_scalar, f"{location}.value.{value_field}")
    if value_field == "bool_value":
        if not isinstance(raw_scalar, bool):
            raise ConfigurationError(f"{location}.value.{value_field} must be a boolean")
        return value_field, raw_scalar
    if value_field == "string_value":
        if not isinstance(raw_scalar, str):
            raise ConfigurationError(f"{location}.value.{value_field} must be a string")
        return value_field, raw_scalar

    return value_field, _parse_rfc3339_timestamp(
        raw_scalar,
        f"{location}.value.{value_field}",
    )


def _parse_quality(raw_quality: Any, location: str) -> dict[str, bool]:
    if raw_quality is None:
        return {}

    quality = _mapping(raw_quality, f"{location}.quality")
    _reject_unknown_keys(quality, QUALITY_FIELDS, f"{location}.quality")
    parsed_quality: dict[str, bool] = {}
    for name, value in quality.items():
        if not isinstance(value, bool):
            raise ConfigurationError(f"{location}.quality.{name} must be a boolean")
        parsed_quality[name] = value
    return parsed_quality


def _parse_rfc3339_timestamp(value: Any, location: str) -> datetime:
    if not isinstance(value, str):
        raise ConfigurationError(f"{location} must be an RFC 3339 timestamp string")

    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ConfigurationError(f"{location} must be an RFC 3339 timestamp string") from error

    if parsed.tzinfo is None:
        raise ConfigurationError(f"{location} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _parse_positive_int(value: Any, location: str) -> int:
    parsed = _parse_non_negative_int(value, location)
    if parsed == 0:
        raise ConfigurationError(f"{location} must be greater than zero")
    return parsed


def _parse_environment_interval(value: str) -> int:
    location = "PMU_GATEWAY_PUBLISH_INTERVAL_MS"
    try:
        parsed = int(value)
    except ValueError as error:
        raise ConfigurationError(f"{location} must be a positive integer") from error
    return _parse_positive_int(parsed, location)


def _parse_non_negative_int(value: Any, location: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ConfigurationError(f"{location} must be a non-negative integer")
    return value


def _parse_int64(value: Any, location: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigurationError(f"{location} must be an integer")
    if not -(2**63) <= value < 2**63:
        raise ConfigurationError(f"{location} must fit in an int64")
    return value


def _parse_uint32(value: Any, location: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigurationError(f"{location} must be an integer")
    if not 0 <= value < 2**32:
        raise ConfigurationError(f"{location} must fit in a uint32")
    return value


def _parse_number(value: Any, location: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ConfigurationError(f"{location} must be a number")
    return float(value)


def _parse_non_negative_finite_number(value: Any, location: str) -> float:
    parsed = _parse_number(value, location)
    if not math.isfinite(parsed) or parsed < 0:
        raise ConfigurationError(f"{location} must be a finite non-negative number")
    return parsed


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a mapping")
    return value


def _reject_unknown_keys(
    mapping: dict[str, Any],
    allowed_keys: set[str] | frozenset[str],
    location: str,
) -> None:
    unknown_keys = set(mapping).difference(allowed_keys)
    if unknown_keys:
        names = ", ".join(sorted(unknown_keys))
        raise ConfigurationError(f"{location} contains unsupported key(s): {names}")