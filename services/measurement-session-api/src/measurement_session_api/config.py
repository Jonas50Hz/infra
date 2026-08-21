"""Environment-backed settings for MeasurementSession request publication."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os
from urllib.parse import urlparse

from measurement_session_common.contract import DEFAULT_MAX_MRIDS


class ConfigurationError(ValueError):
    """Raised when request publication cannot preserve the session contract."""


@dataclass(frozen=True)
class Settings:
    """Bounded request and trusted-local connection settings."""

    kafka_bootstrap_servers: str
    kafka_topic: str
    max_interval_hours: int
    max_mrids: int
    publish_timeout_seconds: int
    grafana_session_dashboard_url: str

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> "Settings":
        """Load local defaults that match the session processor's limits."""

        values = os.environ if environment is None else environment
        return cls(
            kafka_bootstrap_servers=_required(
                values,
                "KAFKA_BOOTSTRAP_SERVERS",
                "kafka:9092",
            ),
            kafka_topic=_required(values, "KAFKA_TOPIC", "MeasurementSession"),
            max_interval_hours=_positive_integer(
                values,
                "MEASUREMENT_SESSION_MAX_INTERVAL_HOURS",
                24,
            ),
            max_mrids=_maximum_mrids(values),
            publish_timeout_seconds=_positive_integer(
                values,
                "KAFKA_PUBLISH_TIMEOUT_SECONDS",
                30,
            ),
            grafana_session_dashboard_url=_grafana_session_dashboard_url(values),
        )


def _required(values: Mapping[str, str], name: str, default: str) -> str:
    value = values.get(name, default).strip()
    if not value:
        raise ConfigurationError(f"{name} must not be empty")
    return value


def _positive_integer(values: Mapping[str, str], name: str, default: int) -> int:
    try:
        value = int(values.get(name, str(default)))
    except ValueError as error:
        raise ConfigurationError(f"{name} must be an integer") from error
    if value < 1:
        raise ConfigurationError(f"{name} must be greater than zero")
    return value


def _maximum_mrids(values: Mapping[str, str]) -> int:
    value = _positive_integer(values, "MEASUREMENT_SESSION_MAX_MRIDS", DEFAULT_MAX_MRIDS)
    if value > DEFAULT_MAX_MRIDS:
        raise ConfigurationError(
            f"MEASUREMENT_SESSION_MAX_MRIDS must not exceed {DEFAULT_MAX_MRIDS}"
        )
    return value


def _http_url(values: Mapping[str, str], name: str, default: str) -> str:
    value = _required(values, name, default)
    return _http_url_value(value, name)


def _grafana_session_dashboard_url(values: Mapping[str, str]) -> str:
    explicit_url = values.get("GRAFANA_SESSION_DASHBOARD_URL", "").strip()
    if explicit_url:
        return _http_url_value(explicit_url, "GRAFANA_SESSION_DASHBOARD_URL")

    grafana_root_url = _http_url(values, "GRAFANA_ROOT_URL", "http://localhost:3001/")
    return f"{grafana_root_url}/d/wama-measurement-sessions/wama-measurement-sessions"


def _http_url_value(value: str, name: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ConfigurationError(f"{name} must be an absolute HTTP(S) URL")
    return value.rstrip("/")