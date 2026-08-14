"""Environment-backed configuration for the infrastructure readiness probe."""

from __future__ import annotations

from dataclasses import dataclass
import os
from collections.abc import Mapping


class ConfigurationError(ValueError):
    """Raised when readiness configuration is incomplete or invalid."""


@dataclass(frozen=True)
class Settings:
    """External endpoints and expectations for the current Compose topology."""

    forgejo_admin_password: str
    forgejo_admin_username: str
    forgejo_application_repository: str
    forgejo_url: str
    grafana_password: str
    grafana_url: str
    grafana_username: str
    kafka_bootstrap_servers: str
    kafka_consume_timeout_seconds: int
    kafka_topic: str
    pmu_expected_mrid_prefix: str
    postgres_database: str
    postgres_dsn: str
    postgres_user: str
    readiness_retry_interval_seconds: int
    readiness_timeout_seconds: int
    s3_access_key_id: str
    s3_buckets: tuple[str, ...]
    s3_endpoint_url: str
    s3_region: str
    s3_secret_access_key: str
    victoria_metrics_url: str

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> Settings:
        """Build settings from an environment mapping, applying local PoC defaults."""

        values = os.environ if environment is None else environment
        return cls(
            forgejo_admin_password=_required(
                values,
                "FORGEJO_ADMIN_PASSWORD",
                "wama-admin",
            ),
            forgejo_admin_username=_required(
                values,
                "FORGEJO_ADMIN_USERNAME",
                "wama-admin",
            ),
            forgejo_application_repository=_required(
                values,
                "FORGEJO_APPLICATION_REPOSITORY",
                "wama-applications",
            ),
            forgejo_url=_url(values, "FORGEJO_URL", "http://forgejo:3000"),
            grafana_password=_required(values, "GF_SECURITY_ADMIN_PASSWORD", "wama-admin"),
            grafana_url=_url(values, "GRAFANA_URL", "http://grafana:3000"),
            grafana_username=_required(values, "GF_SECURITY_ADMIN_USER", "wama-admin"),
            kafka_bootstrap_servers=_required(
                values,
                "KAFKA_BOOTSTRAP_SERVERS",
                "kafka:9092",
            ),
            kafka_consume_timeout_seconds=_positive_integer(
                values,
                "KAFKA_CONSUME_TIMEOUT_SECONDS",
                12,
            ),
            kafka_topic=_required(values, "KAFKA_TOPIC", "LiveMeasurement"),
            pmu_expected_mrid_prefix=values.get(
                "PMU_EXPECTED_MRID_PREFIX",
                "urn:wama:poc:pmu:",
            ).strip(),
            postgres_database=_required(values, "POSTGRES_DATABASE", "wama"),
            postgres_dsn=_required(
                values,
                "POSTGRES_DSN",
                "postgresql://wama:wama-postgres-password@postgres:5432/wama",
            ),
            postgres_user=_required(values, "POSTGRES_USER", "wama"),
            readiness_retry_interval_seconds=_positive_integer(
                values,
                "READINESS_RETRY_INTERVAL_SECONDS",
                2,
            ),
            readiness_timeout_seconds=_positive_integer(
                values,
                "READINESS_TIMEOUT_SECONDS",
                180,
            ),
            s3_access_key_id=_required(values, "S3_ACCESS_KEY_ID", "wama-s3-admin"),
            s3_buckets=_buckets(values),
            s3_endpoint_url=_url(values, "S3_ENDPOINT_URL", "http://seaweedfs:8333"),
            s3_region=_required(values, "S3_REGION", "us-east-1"),
            s3_secret_access_key=_required(
                values,
                "S3_SECRET_ACCESS_KEY",
                "wama-s3-admin-secret",
            ),
            victoria_metrics_url=_url(
                values,
                "VICTORIA_METRICS_URL",
                "http://victoria-metrics:8428",
            ),
        )


def _buckets(values: Mapping[str, str]) -> tuple[str, ...]:
    buckets = tuple(
        bucket.strip()
        for bucket in values.get("S3_BUCKETS", "wama-raw,wama-measurement-sessions").split(",")
        if bucket.strip()
    )
    if not buckets:
        raise ConfigurationError("S3_BUCKETS must name at least one bucket")
    return buckets


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