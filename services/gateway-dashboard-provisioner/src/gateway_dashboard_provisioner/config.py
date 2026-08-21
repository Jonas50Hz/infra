"""Environment-backed settings for gateway dashboard reconciliation."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os


class ConfigurationError(ValueError):
    """Raised when reconciliation settings are incomplete or unsafe."""


@dataclass(frozen=True)
class Settings:
    """Kafka and Grafana-file locations owned by the root dashboard service."""

    dashboard_directory: str
    kafka_bootstrap_servers: str
    kafka_retry_interval_seconds: int
    kafka_topic: str
    ready_file: str

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> "Settings":
        """Read trusted-PoC defaults while rejecting blank or invalid settings."""

        values = os.environ if environment is None else environment
        return cls(
            dashboard_directory=_required(
                values,
                "GRAFANA_DASHBOARD_DIRECTORY",
                "/var/lib/wama-gateway-dashboards",
            ),
            kafka_bootstrap_servers=_required(values, "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            kafka_retry_interval_seconds=_positive_integer(
                values,
                "KAFKA_RETRY_INTERVAL_SECONDS",
                2,
            ),
            kafka_topic=_required(values, "KAFKA_TOPIC", "Masterdata"),
            ready_file=_required(
                values,
                "GATEWAY_DASHBOARD_READY_FILE",
                "/tmp/wama-gateway-dashboard-ready",
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