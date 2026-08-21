"""Environment-backed configuration for the infrastructure readiness probe."""

from __future__ import annotations

from dataclasses import dataclass
import math
import os
from collections.abc import Mapping

DEFAULT_FORGEJO_MANAGED_REPOSITORIES = (
    "processor-frequency-scale",
    "processor-apparent-power",
    "processor-frequency-iec104-export",
    "processor-lfr-frequency-provision",
    "gateway-c37-118-onboarding",
)


class ConfigurationError(ValueError):
    """Raised when readiness configuration is incomplete or invalid."""


@dataclass(frozen=True)
class Settings:
    """External endpoints and expectations for the current Compose topology."""

    druid_datasource: str
    druid_expected_double_value: float
    druid_expected_double_value_tolerance: float
    druid_expected_mrid: str
    druid_router_url: str
    druid_supervisor_id: str
    iec104_browser_url: str
    measurement_session_api_url: str
    measurement_session_exporter_url: str
    forgejo_admin_password: str
    forgejo_admin_username: str
    forgejo_managed_repositories: tuple[str, ...]
    forgejo_url: str
    grafana_password: str
    grafana_url: str
    grafana_username: str
    blobmeta_topic_partitions: int
    kafka_bootstrap_servers: str
    kafka_consume_timeout_seconds: int
    kafka_topic: str
    measurement_session_topic_partitions: int
    pmu_expected_mrid_prefix: str
    require_live_measurement: bool
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
    trino_blobmeta_catalog: str
    trino_blobmeta_schema: str
    trino_druid_catalog: str
    trino_druid_schema: str
    trino_session_catalog: str
    trino_session_schema: str
    trino_session_table: str
    trino_url: str
    trino_user: str
    victoria_metrics_url: str

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> Settings:
        """Build settings from an environment mapping, applying local PoC defaults."""

        values = os.environ if environment is None else environment
        return cls(
            druid_datasource=_identifier(values, "DRUID_DATASOURCE", "live_measurements"),
            druid_expected_double_value=_finite_float(
                values,
                "DRUID_EXPECTED_DOUBLE_VALUE",
                50.01,
            ),
            druid_expected_double_value_tolerance=_non_negative_finite_float(
                values,
                "DRUID_EXPECTED_DOUBLE_VALUE_TOLERANCE",
                0.01,
            ),
            druid_expected_mrid=_required(
                values,
                "DRUID_EXPECTED_MRID",
                "urn:wama:poc:pmu:bay-01:frequency",
            ),
            druid_router_url=_url(values, "DRUID_ROUTER_URL", "http://druid:8888"),
            druid_supervisor_id=_identifier(
                values,
                "DRUID_SUPERVISOR_ID",
                "live_measurements",
            ),
            iec104_browser_url=_url(values, "IEC104_BROWSER_URL", "http://iec104-browser:8080"),
            measurement_session_api_url=_url(
                values,
                "MEASUREMENT_SESSION_API_URL",
                "http://measurement-session-api:8080",
            ),
            measurement_session_exporter_url=_url(
                values,
                "MEASUREMENT_SESSION_EXPORTER_URL",
                "http://measurement-session-exporter:8080",
            ),
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
            forgejo_managed_repositories=_repositories(
                values,
                "FORGEJO_MANAGED_REPOSITORIES",
                DEFAULT_FORGEJO_MANAGED_REPOSITORIES,
            ),
            forgejo_url=_url(values, "FORGEJO_URL", "http://forgejo:3000"),
            grafana_password=_required(values, "GF_SECURITY_ADMIN_PASSWORD", "wama-admin"),
            grafana_url=_url(values, "GRAFANA_URL", "http://grafana:3000"),
            grafana_username=_required(values, "GF_SECURITY_ADMIN_USER", "wama-admin"),
            blobmeta_topic_partitions=_positive_integer(
                values,
                "BLOBMETA_TOPIC_PARTITIONS",
                12,
            ),
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
            measurement_session_topic_partitions=_positive_integer(
                values,
                "MEASUREMENT_SESSION_TOPIC_PARTITIONS",
                12,
            ),
            pmu_expected_mrid_prefix=values.get(
                "PMU_EXPECTED_MRID_PREFIX",
                "urn:wama:poc:pmu:",
            ).strip(),
            require_live_measurement=_boolean(
                values,
                "REQUIRE_LIVE_MEASUREMENT",
                False,
            ),
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
            trino_blobmeta_catalog=_identifier(
                values,
                "TRINO_BLOBMETA_CATALOG",
                "blobmeta",
            ),
            trino_blobmeta_schema=_identifier(
                values,
                "TRINO_BLOBMETA_SCHEMA",
                "blobmeta_catalog",
            ),
            trino_druid_catalog=_identifier(
                values,
                "TRINO_DRUID_CATALOG",
                "druid",
            ),
            trino_druid_schema=_identifier(
                values,
                "TRINO_DRUID_SCHEMA",
                "druid",
            ),
            trino_session_catalog=_identifier(
                values,
                "TRINO_SESSION_CATALOG",
                "sessions",
            ),
            trino_session_schema=_identifier(
                values,
                "TRINO_SESSION_SCHEMA",
                "wama",
            ),
            trino_session_table=_identifier(
                values,
                "TRINO_SESSION_TABLE",
                "measurement_values",
            ),
            trino_url=_url(values, "TRINO_URL", "http://trino:8080"),
            trino_user=_required(values, "TRINO_USER", "wama"),
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


def _repositories(
    values: Mapping[str, str],
    name: str,
    default: tuple[str, ...],
) -> tuple[str, ...]:
    repositories = tuple(
        repository.strip()
        for repository in values.get(name, ",".join(default)).split(",")
        if repository.strip()
    )
    if not repositories:
        raise ConfigurationError(f"{name} must name at least one repository")
    if len(set(repositories)) != len(repositories):
        raise ConfigurationError(f"{name} must not repeat repositories")
    for repository in repositories:
        normalized = repository.replace("-", "").replace("_", "").replace(".", "")
        if not normalized.isalnum():
            raise ConfigurationError(
                f"{name} repositories must use letters, numbers, dots, underscores, or hyphens"
            )
    return repositories


def _finite_float(values: Mapping[str, str], name: str, default: float) -> float:
    raw_value = values.get(name, str(default)).strip()
    try:
        value = float(raw_value)
    except ValueError as error:
        raise ConfigurationError(f"{name} must be a number") from error
    if not math.isfinite(value):
        raise ConfigurationError(f"{name} must be finite")
    return value


def _non_negative_finite_float(
    values: Mapping[str, str],
    name: str,
    default: float,
) -> float:
    value = _finite_float(values, name, default)
    if value < 0:
        raise ConfigurationError(f"{name} must be non-negative")
    return value


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


def _boolean(values: Mapping[str, str], name: str, default: bool) -> bool:
    raw_value = values.get(name, str(default).lower()).strip().lower()
    if raw_value not in {"true", "false"}:
        raise ConfigurationError(f"{name} must be true or false")
    return raw_value == "true"


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