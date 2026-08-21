"""Environment-backed settings for the session query indexer."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os


class ConfigurationError(ValueError):
    """Raised when indexer settings are missing or unsafe."""


@dataclass(frozen=True)
class Settings:
    """Kafka, SeaweedFS, PostgreSQL, and internal Trino writer settings."""

    kafka_bootstrap_servers: str
    kafka_auto_offset_reset: str
    kafka_consumer_group: str
    kafka_retry_interval_seconds: int
    kafka_topic: str
    postgres_dsn: str
    s3_access_key_id: str
    s3_endpoint_url: str
    s3_region: str
    s3_secret_access_key: str
    trino_writer_url: str
    trino_writer_user: str

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> "Settings":
        """Apply trusted-PoC defaults while rejecting incomplete endpoints."""

        values = os.environ if environment is None else environment
        return cls(
            kafka_bootstrap_servers=_required(values, "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            kafka_auto_offset_reset=_offset_reset(values),
            kafka_consumer_group=_required(
                values,
                "KAFKA_CONSUMER_GROUP",
                "measurement-session-query-indexer",
            ),
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
            s3_access_key_id=_required(values, "S3_ACCESS_KEY_ID", "wama-s3-admin"),
            s3_endpoint_url=_url(values, "S3_ENDPOINT_URL", "http://seaweedfs:8333"),
            s3_region=_required(values, "S3_REGION", "us-east-1"),
            s3_secret_access_key=_required(
                values,
                "S3_SECRET_ACCESS_KEY",
                "wama-s3-admin-secret",
            ),
            trino_writer_url=_url(
                values,
                "TRINO_WRITER_URL",
                "http://trino-session-writer:8080",
            ),
            trino_writer_user=_required(
                values,
                "TRINO_WRITER_USER",
                "wama-session-query-indexer",
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


def _offset_reset(values: Mapping[str, str]) -> str:
    value = _required(values, "KAFKA_AUTO_OFFSET_RESET", "earliest")
    if value not in {"earliest", "latest"}:
        raise ConfigurationError("KAFKA_AUTO_OFFSET_RESET must be earliest or latest")
    return value


def _url(values: Mapping[str, str], name: str, default: str) -> str:
    value = _required(values, name, default).rstrip("/")
    if not value.startswith(("http://", "https://")):
        raise ConfigurationError(f"{name} must be an HTTP URL")
    return value