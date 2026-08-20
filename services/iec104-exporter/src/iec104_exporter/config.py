"""Environment-backed configuration for the IEC 104 exporter."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os


class ConfigurationError(ValueError):
    """Raised when exporter configuration is incomplete or unsafe."""


@dataclass(frozen=True)
class Settings:
    """Runtime settings for Kafka consumption and the IEC 104 listener."""

    bind_host: str
    backend_port: int
    kafka_bootstrap_servers: str
    kafka_consumer_group: str
    kafka_topic: str
    port: int
    ready_file: str
    retry_interval_seconds: float

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> "Settings":
        """Load strict, documented local-PoC settings from environment values."""

        values = os.environ if environment is None else environment
        port = _port(values, "IEC104_PORT", 2404)
        backend_port = _port(values, "IEC104_BACKEND_PORT", 2405)
        if backend_port == port:
            raise ConfigurationError("IEC104_BACKEND_PORT must differ from IEC104_PORT")
        return cls(
            bind_host=_required(values, "IEC104_BIND_HOST", "0.0.0.0"),
            backend_port=backend_port,
            kafka_bootstrap_servers=_required(values, "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            kafka_consumer_group=_required(values, "KAFKA_CONSUMER_GROUP", "iec104-exporter"),
            kafka_topic=_required(values, "KAFKA_TOPIC", "Export"),
            port=port,
            ready_file=_absolute_path(values, "IEC104_READY_FILE", "/tmp/iec104-exporter-ready"),
            retry_interval_seconds=_positive_float(values, "IEC104_RETRY_INTERVAL_SECONDS", 2.0),
        )


def _required(values: Mapping[str, str], name: str, default: str) -> str:
    value = values.get(name, default).strip()
    if not value:
        raise ConfigurationError(f"{name} must not be empty")
    return value


def _port(values: Mapping[str, str], name: str, default: int) -> int:
    try:
        value = int(values.get(name, str(default)))
    except ValueError as error:
        raise ConfigurationError(f"{name} must be an integer") from error
    if not 1 <= value <= 65_535:
        raise ConfigurationError(f"{name} must be between 1 and 65535")
    return value


def _absolute_path(values: Mapping[str, str], name: str, default: str) -> str:
    value = _required(values, name, default)
    if not value.startswith("/"):
        raise ConfigurationError(f"{name} must be an absolute path")
    return value


def _positive_float(values: Mapping[str, str], name: str, default: float) -> float:
    try:
        value = float(values.get(name, str(default)))
    except ValueError as error:
        raise ConfigurationError(f"{name} must be a number") from error
    if value <= 0:
        raise ConfigurationError(f"{name} must be greater than zero")
    return value