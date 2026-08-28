"""Environment-backed settings for the persistent IEC 104 browser."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os


class ConfigurationError(ValueError):
    """Raised when browser monitor settings cannot form a safe connection."""


@dataclass(frozen=True)
class Settings:
    """Trusted local connection and transient observer queue settings."""

    exporter_host: str
    exporter_port: int
    queue_size: int

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> "Settings":
        """Load documented local-PoC defaults without storing any event history."""

        values = os.environ if environment is None else environment
        return cls(
            exporter_host=_required(values, "IEC104_EXPORTER_HOST", "iec104-exporter"),
            exporter_port=_port(values, "IEC104_EXPORTER_PORT", 2404),
            queue_size=_positive_integer(values, "IEC104_BROWSER_QUEUE_SIZE", 256),
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


def _positive_integer(values: Mapping[str, str], name: str, default: int) -> int:
    try:
        value = int(values.get(name, str(default)))
    except ValueError as error:
        raise ConfigurationError(f"{name} must be an integer") from error
    if value <= 0:
        raise ConfigurationError(f"{name} must be greater than zero")
    return value