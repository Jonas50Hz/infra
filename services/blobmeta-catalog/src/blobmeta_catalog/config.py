"""Environment-backed settings for the Blobmeta catalog worker."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os


class ConfigurationError(ValueError):
    """Raised when catalog connection settings are incomplete or invalid."""


@dataclass(frozen=True)
class Settings:
    """Kafka and PostgreSQL endpoints owned by the metadata catalog."""

    kafka_bootstrap_servers: str
    kafka_consumer_group: str
    kafka_retry_interval_seconds: int
    kafka_topic: str
    postgres_dsn: str

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> "Settings":
        """Apply trusted-PoC defaults while rejecting unsafe empty settings."""

        values = os.environ if environment is None else environment
        return cls(
            kafka_bootstrap_servers=_required(values, "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            kafka_consumer_group=_required(values, "KAFKA_CONSUMER_GROUP", "blobmeta-catalog"),
            kafka_retry_interval_seconds=_positive_integer(
                values,
                "KAFKA_RETRY_INTERVAL_SECONDS",
                2,
            ),
            kafka_topic=_required(values, "KAFKA_TOPIC", "Blobmeta"),
            postgres_dsn=_required(
                values,
                "POSTGRES_DSN",
                "postgresql://wama:wama-postgres-password@postgres:5432/wama",
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