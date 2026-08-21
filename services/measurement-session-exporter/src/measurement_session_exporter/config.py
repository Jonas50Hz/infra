"""Environment-backed settings for the CSV export surface."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os
from urllib.parse import urlparse


class ConfigurationError(ValueError):
    """Raised when the trusted-local exporter has invalid configuration."""


@dataclass(frozen=True)
class Settings:
    """Read-only Trino connection settings."""

    trino_url: str
    trino_user: str

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> "Settings":
        """Load local defaults for the public read-only Trino coordinator."""

        values = os.environ if environment is None else environment
        return cls(
            trino_url=_http_url(values, "TRINO_URL", "http://trino:8080"),
            trino_user=_required(values, "TRINO_USER", "measurement-session-exporter"),
        )


def _required(values: Mapping[str, str], name: str, default: str) -> str:
    value = values.get(name, default).strip()
    if not value:
        raise ConfigurationError(f"{name} must not be empty")
    return value


def _http_url(values: Mapping[str, str], name: str, default: str) -> str:
    value = _required(values, name, default)
    parsed = urlparse(value)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ConfigurationError(f"{name} must be an absolute HTTP(S) URL")
    return value.rstrip("/")