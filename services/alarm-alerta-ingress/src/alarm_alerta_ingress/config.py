"""Environment-backed settings for the root Alarm-to-Alerta ingress."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os
from urllib.parse import urlparse


class ConfigurationError(ValueError):
    """Raised when ingress settings are unsafe or incomplete."""


@dataclass(frozen=True)
class Settings:
    """Kafka and Alerta endpoints owned by the root ingress service."""

    alerta_api_key: str
    alerta_request_timeout_seconds: int
    alerta_url: str
    kafka_bootstrap_servers: str
    kafka_retry_interval_seconds: int
    kafka_topic: str
    ready_file: str

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> "Settings":
        """Read trusted-PoC defaults while rejecting unsafe overrides."""

        values = os.environ if environment is None else environment
        return cls(
            alerta_api_key=_required(
                values,
                "ALERTA_API_KEY",
                "wama-alerta-ingress-local-api-key-0001",
            ),
            alerta_request_timeout_seconds=_positive_integer(
                values,
                "ALERTA_REQUEST_TIMEOUT_SECONDS",
                10,
            ),
            alerta_url=_url(values, "ALERTA_URL", "http://alerta:8080"),
            kafka_bootstrap_servers=_required(values, "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            kafka_retry_interval_seconds=_positive_integer(
                values,
                "KAFKA_RETRY_INTERVAL_SECONDS",
                2,
            ),
            kafka_topic=_required(values, "KAFKA_TOPIC", "Alarm"),
            ready_file=_required(
                values,
                "ALARM_ALERTA_INGRESS_READY_FILE",
                "/tmp/wama-alarm-alerta-ingress-ready",
            ),
        )


def _positive_integer(values: Mapping[str, str], name: str, default: int) -> int:
    raw_value = values.get(name, str(default)).strip()
    try:
        value = int(raw_value)
    except ValueError as error:
        raise ConfigurationError(f"{name} must be an integer") from error
    if value <= 0:
        raise ConfigurationError(f"{name} must be greater than zero")
    return value


def _required(values: Mapping[str, str], name: str, default: str) -> str:
    value = values.get(name, default).strip()
    if not value:
        raise ConfigurationError(f"{name} must not be empty")
    return value


def _url(values: Mapping[str, str], name: str, default: str) -> str:
    value = _required(values, name, default).rstrip("/")
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ConfigurationError(f"{name} must be an absolute HTTP URL")
    return value