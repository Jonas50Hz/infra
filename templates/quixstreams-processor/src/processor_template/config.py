"""Configuration loading for a provisioned Quixstreams processor."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


class ConfigurationError(ValueError):
    """Raised when a processor configuration cannot be used safely."""


@dataclass(frozen=True)
class ProcessorConfig:
    """Kafka settings owned by one processor service."""

    kafka_bootstrap_servers: str
    consumer_group: str
    input_topic: str
    output_topic: str


def load_config(path: str | Path) -> ProcessorConfig:
    """Load one processor YAML configuration before connecting to Kafka."""

    config_path = Path(path)
    try:
        with config_path.open(encoding="utf-8") as config_file:
            raw_config = yaml.safe_load(config_file)
    except OSError as error:
        raise ConfigurationError(
            f"Unable to read processor configuration {config_path}: {error}"
        ) from error
    except yaml.YAMLError as error:
        raise ConfigurationError(
            f"Unable to parse processor configuration {config_path}: {error}"
        ) from error

    config = _mapping(raw_config, "configuration")
    _reject_unknown_keys(
        config,
        {
            "kafka_bootstrap_servers",
            "consumer_group",
            "input_topic",
            "output_topic",
        },
    )

    return ProcessorConfig(
        kafka_bootstrap_servers=_required_string(
            config,
            "kafka_bootstrap_servers",
        ),
        consumer_group=_required_string(config, "consumer_group"),
        input_topic=_required_string(config, "input_topic"),
        output_topic=_required_string(config, "output_topic"),
    )


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a mapping")
    return value


def _reject_unknown_keys(config: dict[str, Any], allowed_keys: set[str]) -> None:
    unknown_keys = set(config).difference(allowed_keys)
    if unknown_keys:
        names = ", ".join(sorted(unknown_keys))
        raise ConfigurationError(f"configuration contains unsupported key(s): {names}")


def _required_string(config: dict[str, Any], name: str) -> str:
    value = config.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(f"configuration.{name} must be a non-empty string")
    return value.strip()