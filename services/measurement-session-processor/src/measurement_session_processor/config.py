"""Environment-backed settings for the MeasurementSession worker."""

from __future__ import annotations

from dataclasses import dataclass
import os
from collections.abc import Mapping

from measurement_session_common.contract import DEFAULT_MAX_MRIDS, DEFAULT_SESSION_BUCKET


class ConfigurationError(ValueError):
    """Raised when worker settings cannot safely bound a session request."""


@dataclass(frozen=True)
class Settings:
    """Runtime endpoints and resource limits for one persistent worker."""

    blobmeta_topic: str
    druid_datasource: str
    druid_query_timeout_seconds: int
    druid_router_url: str
    kafka_bootstrap_servers: str
    kafka_consumer_group: str
    kafka_retry_interval_seconds: int
    kafka_topic: str
    max_artifact_bytes: int
    max_interval_hours: int
    max_mrids: int
    max_rows: int
    parquet_batch_rows: int
    s3_access_key_id: str
    s3_bucket: str
    s3_endpoint_url: str
    s3_region: str
    s3_secret_access_key: str

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> "Settings":
        """Read settings without embedding trusted-PoC endpoints in code."""

        values = os.environ if environment is None else environment
        bucket = _required(values, "S3_BUCKET", DEFAULT_SESSION_BUCKET)
        if bucket != DEFAULT_SESSION_BUCKET:
            raise ConfigurationError(f"S3_BUCKET must be {DEFAULT_SESSION_BUCKET!r}")
        return cls(
            blobmeta_topic=_required(values, "BLOBMETA_TOPIC", "Blobmeta"),
            druid_datasource=_identifier(values, "DRUID_DATASOURCE", "live_measurements"),
            druid_query_timeout_seconds=_positive_integer(
                values,
                "DRUID_QUERY_TIMEOUT_SECONDS",
                60,
            ),
            druid_router_url=_url(values, "DRUID_ROUTER_URL", "http://druid:8888"),
            kafka_bootstrap_servers=_required(
                values,
                "KAFKA_BOOTSTRAP_SERVERS",
                "kafka:9092",
            ),
            kafka_consumer_group=_required(
                values,
                "KAFKA_CONSUMER_GROUP",
                "measurement-session-processor",
            ),
            kafka_retry_interval_seconds=_positive_integer(
                values,
                "KAFKA_RETRY_INTERVAL_SECONDS",
                2,
            ),
            kafka_topic=_required(values, "KAFKA_TOPIC", "MeasurementSession"),
            max_artifact_bytes=_positive_integer(
                values,
                "MEASUREMENT_SESSION_MAX_ARTIFACT_BYTES",
                4 * 1024 * 1024 * 1024,
            ),
            max_interval_hours=_positive_integer(
                values,
                "MEASUREMENT_SESSION_MAX_INTERVAL_HOURS",
                24,
            ),
            max_mrids=_bounded_positive_integer(
                values,
                "MEASUREMENT_SESSION_MAX_MRIDS",
                DEFAULT_MAX_MRIDS,
                DEFAULT_MAX_MRIDS,
            ),
            max_rows=_positive_integer(values, "MEASUREMENT_SESSION_MAX_ROWS", 5_000_000),
            parquet_batch_rows=_positive_integer(
                values,
                "MEASUREMENT_SESSION_PARQUET_BATCH_ROWS",
                10_000,
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


def _identifier(values: Mapping[str, str], name: str, default: str) -> str:
    value = _required(values, name, default)
    if not value.replace("_", "").isalnum():
        raise ConfigurationError(f"{name} must contain only letters, numbers, and underscores")
    return value


def _positive_integer(values: Mapping[str, str], name: str, default: int) -> int:
    raw_value = values.get(name, str(default)).strip()
    try:
        value = int(raw_value)
    except ValueError as error:
        raise ConfigurationError(f"{name} must be an integer") from error
    if value <= 0:
        raise ConfigurationError(f"{name} must be greater than zero")
    return value


def _bounded_positive_integer(
    values: Mapping[str, str],
    name: str,
    default: int,
    maximum: int,
) -> int:
    """Require a positive integer that remains representable in Blobmeta."""

    value = _positive_integer(values, name, default)
    if value > maximum:
        raise ConfigurationError(f"{name} must not exceed {maximum}")
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