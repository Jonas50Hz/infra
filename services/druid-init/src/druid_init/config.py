"""Environment-backed configuration for Druid supervisor initialization."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os
from pathlib import Path


class ConfigurationError(ValueError):
    """Raised when the Druid initialization configuration is invalid."""


@dataclass(frozen=True)
class Settings:
    """Druid API and retry settings for the live-measurement supervisor."""

    retry_interval_seconds: int
    router_url: str
    supervisor_id: str
    supervisor_spec_path: Path
    timeout_seconds: int

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> Settings:
        """Build settings from an environment mapping with local PoC defaults."""

        values = os.environ if environment is None else environment
        return cls(
            retry_interval_seconds=_positive_integer(
                values,
                "DRUID_INIT_RETRY_INTERVAL_SECONDS",
                2,
            ),
            router_url=_url(values, "DRUID_ROUTER_URL", "http://druid:8888"),
            supervisor_id=_required(values, "DRUID_SUPERVISOR_ID", "live_measurements"),
            supervisor_spec_path=_absolute_path(
                values,
                "DRUID_SUPERVISOR_SPEC_PATH",
                "/etc/wama/supervisors/live-measurements.json",
            ),
            timeout_seconds=_positive_integer(values, "DRUID_INIT_TIMEOUT_SECONDS", 240),
        )


def _absolute_path(values: Mapping[str, str], name: str, default: str) -> Path:
    path = Path(_required(values, name, default))
    if not path.is_absolute():
        raise ConfigurationError(f"{name} must be an absolute path")
    return path


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
    if not value.startswith(("http://", "https://")):
        raise ConfigurationError(f"{name} must be an HTTP URL")
    return value