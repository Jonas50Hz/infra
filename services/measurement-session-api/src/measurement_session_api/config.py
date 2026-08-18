"""Environment-backed settings for the finalized-session catalog API."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os

from measurement_session_common.contract import DEFAULT_SESSION_BUCKET


class ConfigurationError(ValueError):
    """Raised when catalog API configuration is incomplete or unsafe."""


@dataclass(frozen=True)
class Settings:
    """Trusted-PoC runtime connections owned by the catalog API."""

    kafka_bootstrap_servers: str
    kafka_consumer_group: str
    kafka_topic: str
    postgres_dsn: str
    s3_access_key_id: str
    s3_bucket: str
    s3_endpoint_url: str
    s3_region: str
    s3_secret_access_key: str

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> "Settings":
        """Build settings with the documented local-PoC defaults."""

        values = os.environ if environment is None else environment
        bucket = _required(values, "S3_BUCKET", DEFAULT_SESSION_BUCKET)
        if bucket != DEFAULT_SESSION_BUCKET:
            raise ConfigurationError(f"S3_BUCKET must be {DEFAULT_SESSION_BUCKET!r}")
        return cls(
            kafka_bootstrap_servers=_required(values, "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            kafka_consumer_group=_required(
                values,
                "KAFKA_CONSUMER_GROUP",
                "measurement-session-catalog-api",
            ),
            kafka_topic=_required(values, "KAFKA_TOPIC", "MeasurementSession"),
            postgres_dsn=_required(
                values,
                "POSTGRES_DSN",
                "postgresql://wama:wama-postgres-password@postgres:5432/wama",
            ),
            s3_access_key_id=_required(values, "S3_ACCESS_KEY_ID", "wama-s3-admin"),
            s3_bucket=bucket,
            s3_endpoint_url=_url(values, "S3_ENDPOINT_URL", "http://seaweedfs:8333"),
            s3_region=_required(values, "S3_REGION", "us-east-1"),
            s3_secret_access_key=_required(
                values,
                "S3_SECRET_ACCESS_KEY",
                "wama-s3-admin-secret",
            ),
        )


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