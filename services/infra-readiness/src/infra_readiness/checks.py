"""Behavioral checks for the WAMA Compose infrastructure."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
import logging
import math
from typing import Any
from urllib.parse import quote
from uuid import uuid4

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError
from google.protobuf.message import DecodeError
from kafka import KafkaConsumer
from kafka.admin import ConfigResource, ConfigResourceType, KafkaAdminClient
from kafka.errors import KafkaError
import psycopg
import requests

from infra_readiness.config import Settings
from infra_readiness.druid import DruidReadinessError, check_druid
from infra_readiness.generated import rtd_schema_pb2

LOGGER = logging.getLogger(__name__)

STREAM_TOPICS = ("LiveMeasurement", "MeasurementSession", "Alarm", "Export")
COMPACTED_TOPICS = ("Masterdata", "Schema", "Blobmeta")
TOPIC_CLEANUP_POLICIES = {
    **{topic: "delete" for topic in STREAM_TOPICS},
    **{topic: "compact" for topic in COMPACTED_TOPICS},
}
REQUIRED_TOPICS = tuple(TOPIC_CLEANUP_POLICIES)
MONITORING_JOBS = (
    "victoria-metrics",
    "grafana",
    "node-exporter",
    "cadvisor",
    "kafka-exporter",
)
GRAFANA_VICTORIA_METRICS_DATASOURCE_UID = "victoriametrics"
GRAFANA_VICTORIA_METRICS_DATASOURCE_TYPE = "prometheus"
GRAFANA_DRUID_DATASOURCE_UID = "druid"
GRAFANA_DRUID_DATASOURCE_TYPE = "grafadruid-druid-datasource"
GRAFANA_DRUID_DATASOURCE_URL = "http://druid:8888"
GRAFANA_KAFKA_OPERATIONS_DASHBOARD_UID = "wama-kafka-operations"
GRAFANA_PMU_MEASUREMENTS_DASHBOARD_UID = "wama-pmu-live-measurements"


class ReadinessError(RuntimeError):
    """Raised when an infrastructure dependency is not ready."""


def check_all(settings: Settings) -> None:
    """Run every externally observable readiness check."""

    check_kafka_topics(settings)
    check_live_measurement(settings)
    try:
        check_druid(settings)
    except DruidReadinessError as error:
        raise ReadinessError(str(error)) from error
    check_postgres(settings)
    check_s3(settings)
    check_forgejo(settings)
    check_kafka_ui(settings)
    check_grafana(settings)
    check_victoria_metrics(settings)


def check_kafka_topics(settings: Settings) -> None:
    """Require the full WAMA topic contract and cleanup policies."""

    admin: KafkaAdminClient | None = None
    try:
        admin = KafkaAdminClient(
            bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
            client_id="infra-readiness",
            api_version_auto_timeout_ms=10_000,
            request_timeout_ms=10_000,
        )
        descriptions = admin.describe_topics(list(REQUIRED_TOPICS))
        resources = [
            ConfigResource(ConfigResourceType.TOPIC, topic)
            for topic in REQUIRED_TOPICS
        ]
        responses = admin.describe_configs(resources)
    except KafkaError as error:
        raise ReadinessError(f"Kafka topic probe failed: {error}") from error
    finally:
        if admin is not None:
            admin.close()

    validate_topic_descriptions(descriptions)
    validate_topic_configurations(topic_configurations_from_responses(responses))


def validate_topic_descriptions(descriptions: Sequence[Mapping[str, Any]]) -> None:
    """Validate that every topic has a single healthy replica and partition."""

    by_topic = {str(description.get("topic")): description for description in descriptions}
    for topic in REQUIRED_TOPICS:
        description = by_topic.get(topic)
        if description is None:
            raise ReadinessError(f"Kafka topic {topic} is missing")
        if description.get("error_code") != 0:
            raise ReadinessError(
                f"Kafka topic {topic} has error code {description.get('error_code')}"
            )

        partitions = description.get("partitions", [])
        if len(partitions) != 1:
            raise ReadinessError(f"Kafka topic {topic} must have one partition")
        partition = partitions[0]
        replicas = partition.get("replicas", [])
        in_sync_replicas = partition.get("isr", [])
        if len(replicas) != 1 or len(in_sync_replicas) != 1:
            raise ReadinessError(
                f"Kafka topic {topic} must have one replica and one in-sync replica"
            )


def topic_configurations_from_responses(responses: Iterable[Any]) -> dict[str, dict[str, str]]:
    """Translate kafka-python DescribeConfigs responses to a topic/config mapping."""

    configurations: dict[str, dict[str, str]] = {}
    for response in responses:
        for resource in response.resources:
            error_code = _response_value(resource, "error_code", 0)
            topic = str(_response_value(resource, "resource_name", 3))
            if error_code != 0:
                raise ReadinessError(
                    f"Kafka topic {topic} configuration has error code {error_code}"
                )
            entries = _response_value(resource, "config_entries", 4)
            configurations[topic] = {
                str(_response_value(entry, "config_names", 0)): str(
                    _response_value(entry, "config_value", 1)
                )
                for entry in entries
            }
    return configurations


def _response_value(value: Any, attribute: str, tuple_index: int) -> Any:
    """Read kafka-python fields exposed as either named objects or plain tuples."""

    if hasattr(value, attribute):
        return getattr(value, attribute)
    return value[tuple_index]


def validate_topic_configurations(configurations: Mapping[str, Mapping[str, str]]) -> None:
    """Require the cleanup policy assigned by the idempotent topic initializer."""

    for topic, expected_cleanup_policy in TOPIC_CLEANUP_POLICIES.items():
        actual_cleanup_policy = configurations.get(topic, {}).get("cleanup.policy")
        if actual_cleanup_policy != expected_cleanup_policy:
            raise ReadinessError(
                f"Kafka topic {topic} must use cleanup.policy={expected_cleanup_policy}; "
                f"found {actual_cleanup_policy!r}"
            )


def check_live_measurement(settings: Settings) -> None:
    """Consume one fresh raw-Protobuf PMU measurement without committing an offset."""

    consumer: KafkaConsumer | None = None
    try:
        consumer = KafkaConsumer(
            settings.kafka_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
            client_id="infra-readiness",
            group_id=f"infra-readiness-{uuid4().hex}",
            enable_auto_commit=False,
            auto_offset_reset="latest",
            consumer_timeout_ms=settings.kafka_consume_timeout_seconds * 1_000,
            request_timeout_ms=30_000,
            api_version_auto_timeout_ms=10_000,
        )
        for record in consumer:
            validate_live_measurement(record, settings.pmu_expected_mrid_prefix)
            return
    except KafkaError as error:
        raise ReadinessError(f"Kafka LiveMeasurement probe failed: {error}") from error
    finally:
        if consumer is not None:
            consumer.close(autocommit=False)

    raise ReadinessError(
        f"Kafka topic {settings.kafka_topic} did not produce a PMU measurement within "
        f"{settings.kafka_consume_timeout_seconds} seconds"
    )


def validate_live_measurement(record: Any, expected_mrid_prefix: str) -> None:
    """Validate one Kafka record against the Common Format PMU contract."""

    message = rtd_schema_pb2.MCCSMeasurementValue()
    try:
        message.ParseFromString(record.value)
    except DecodeError as error:
        raise ReadinessError("LiveMeasurement payload is not valid MCCSMeasurementValue Protobuf") from error

    if not message.mrid:
        raise ReadinessError("LiveMeasurement MCCSMeasurementValue has no MRID")
    if expected_mrid_prefix and not message.mrid.startswith(expected_mrid_prefix):
        raise ReadinessError(
            f"LiveMeasurement MRID {message.mrid!r} does not start with "
            f"{expected_mrid_prefix!r}"
        )
    if record.key != message.mrid.encode("utf-8"):
        raise ReadinessError("LiveMeasurement Kafka key does not match its MCCS MRID")
    if message.WhichOneof("value") is None:
        raise ReadinessError("LiveMeasurement MCCSMeasurementValue has no typed value")
    if not message.HasField("quality") or not message.quality.HasField("valid"):
        raise ReadinessError("LiveMeasurement MCCSMeasurementValue has no valid quality flag")
    if not message.quality.valid:
        raise ReadinessError("LiveMeasurement MCCSMeasurementValue is not valid")

    timestamp_names = ("timestamp_field", "timestamp_gateway", "timestamp_mccs")
    for timestamp_name in timestamp_names:
        if not message.HasField(timestamp_name):
            raise ReadinessError(f"LiveMeasurement MCCSMeasurementValue has no {timestamp_name}")
    field_timestamp = _timestamp_nanoseconds(message.timestamp_field)
    gateway_timestamp = _timestamp_nanoseconds(message.timestamp_gateway)
    mccs_timestamp = _timestamp_nanoseconds(message.timestamp_mccs)
    if not field_timestamp <= gateway_timestamp <= mccs_timestamp:
        raise ReadinessError("LiveMeasurement timestamps are not field <= gateway <= MCCS")
    if record.timestamp != mccs_timestamp // 1_000_000:
        raise ReadinessError("LiveMeasurement Kafka timestamp does not match timestamp_mccs")


def _timestamp_nanoseconds(timestamp: Any) -> int:
    return timestamp.seconds * 1_000_000_000 + timestamp.nanos


def check_postgres(settings: Settings) -> None:
    """Verify the prepared PostgreSQL target is reachable with its expected identity."""

    try:
        with psycopg.connect(settings.postgres_dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT current_database(), current_user;")
                identity = cursor.fetchone()
    except (OSError, psycopg.Error) as error:
        raise ReadinessError(f"PostgreSQL probe failed: {error}") from error

    if identity != (settings.postgres_database, settings.postgres_user):
        raise ReadinessError(
            "PostgreSQL identity does not match the prepared WAMA target: "
            f"{identity!r}"
        )


def check_s3(settings: Settings) -> None:
    """Exercise signed SeaweedFS S3 access without retaining probe data."""

    client = boto3.client(
        "s3",
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        endpoint_url=settings.s3_endpoint_url,
        region_name=settings.s3_region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    payload = b"wama infrastructure readiness\n"
    for bucket in settings.s3_buckets:
        _check_s3_bucket(client, bucket, payload)


def _check_s3_bucket(client: Any, bucket: str, payload: bytes) -> None:
    object_key = f"infra-readiness/{uuid4().hex}.txt"
    object_created = False
    failure: ReadinessError | None = None
    try:
        client.head_bucket(Bucket=bucket)
        client.put_object(Bucket=bucket, Key=object_key, Body=payload)
        object_created = True
        response = client.get_object(Bucket=bucket, Key=object_key)
        body = response["Body"]
        try:
            actual_payload = body.read()
        finally:
            body.close()
        if actual_payload != payload:
            failure = ReadinessError(f"S3 bucket {bucket} returned unexpected probe content")
    except (BotoCoreError, ClientError, OSError) as error:
        failure = ReadinessError(f"S3 probe failed for bucket {bucket}: {error}")
    finally:
        if object_created:
            try:
                client.delete_object(Bucket=bucket, Key=object_key)
            except (BotoCoreError, ClientError, OSError) as error:
                cleanup_error = ReadinessError(
                    f"S3 probe cleanup failed for bucket {bucket}: {error}"
                )
                failure = failure or cleanup_error
    if failure is not None:
        raise failure


def check_forgejo(settings: Settings) -> None:
    """Verify Forgejo, its bootstrapped private repository, and its runner."""

    session = requests.Session()
    try:
        _request_json(session, "Forgejo health", _url(settings.forgejo_url, "/api/healthz"))
        encoded_owner = quote(settings.forgejo_admin_username, safe="")
        encoded_repository = quote(settings.forgejo_processors_repository, safe="")
        auth = (settings.forgejo_admin_username, settings.forgejo_admin_password)
        repository = _request_json(
            session,
            "Forgejo processors repository",
            _url(settings.forgejo_url, f"/api/v1/repos/{encoded_owner}/{encoded_repository}"),
            auth=auth,
        )
        validate_forgejo_repository(repository)
        runners = _request_json(
            session,
            "Forgejo processors runners",
            _url(
                settings.forgejo_url,
                f"/api/v1/repos/{encoded_owner}/{encoded_repository}/actions/runners",
            ),
            auth=auth,
        )
    finally:
        session.close()

    validate_forgejo_runners(runners)


def validate_forgejo_repository(payload: Any) -> None:
    """Require the processors repository to be private and seeded on main."""

    if (
        not isinstance(payload, Mapping)
        or not payload.get("private")
        or payload.get("empty") is not False
        or payload.get("default_branch") != "main"
    ):
        raise ReadinessError("Forgejo processors repository must be private and seeded on main")


def validate_forgejo_runners(payload: Any) -> None:
    """Require usable, separately scoped CI and deployment runner connections."""

    if not isinstance(payload, list):
        raise ReadinessError("Forgejo runner endpoint did not return a list")
    expected_connections = {
        "wama-processors-ci": "wama-processors-ci",
        "wama-processors-deploy": "wama-processors-deploy",
    }
    runners_by_name = {
        runner.get("name"): runner
        for runner in payload
        if isinstance(runner, Mapping) and isinstance(runner.get("name"), str)
    }
    for runner_name, required_label in expected_connections.items():
        runner = runners_by_name.get(runner_name)
        if runner is None:
            raise ReadinessError(f"Forgejo runner {runner_name!r} is not registered")
        if runner.get("status") not in {"active", "idle", "online"}:
            raise ReadinessError(
                f"Forgejo runner {runner_name!r} is not online: {runner.get('status')!r}"
            )
        labels = {
            label.split(":", 1)[0]
            for label in runner.get("labels", [])
            if isinstance(label, str)
        }
        if required_label not in labels:
            raise ReadinessError(
                f"Forgejo runner {runner_name!r} is missing label {required_label!r}"
            )


def check_kafka_ui(settings: Settings) -> None:
    """Require Kafka UI to expose an operational health endpoint."""

    session = requests.Session()
    try:
        payload = _request_json(
            session,
            "Kafka UI health",
            "http://kafka-ui:8080/actuator/health",
        )
    finally:
        session.close()
    if not isinstance(payload, Mapping) or str(payload.get("status", "")).upper() != "UP":
        raise ReadinessError("Kafka UI health endpoint did not report UP")


def check_grafana(settings: Settings) -> None:
    """Require Grafana health plus provisioned infrastructure and PMU views."""

    session = requests.Session()
    auth = (settings.grafana_username, settings.grafana_password)
    try:
        _request_json(session, "Grafana health", _url(settings.grafana_url, "/api/health"))
        datasource = _request_json(
            session,
            "Grafana VictoriaMetrics datasource",
            _url(
                settings.grafana_url,
                f"/api/datasources/uid/{GRAFANA_VICTORIA_METRICS_DATASOURCE_UID}",
            ),
            auth=auth,
        )
        druid_datasource = _request_json(
            session,
            "Grafana Druid datasource",
            _url(settings.grafana_url, f"/api/datasources/uid/{GRAFANA_DRUID_DATASOURCE_UID}"),
            auth=auth,
        )
        dashboard = _request_json(
            session,
            "Grafana Kafka Operations dashboard",
            _url(
                settings.grafana_url,
                f"/api/dashboards/uid/{GRAFANA_KAFKA_OPERATIONS_DASHBOARD_UID}",
            ),
            auth=auth,
        )
        pmu_dashboard = _request_json(
            session,
            "Grafana PMU Live Measurements dashboard",
            _url(
                settings.grafana_url,
                f"/api/dashboards/uid/{GRAFANA_PMU_MEASUREMENTS_DASHBOARD_UID}",
            ),
            auth=auth,
        )
        pmu_query = _post_json(
            session,
            "Grafana Druid PMU query",
            _url(settings.grafana_url, "/api/ds/query"),
            _grafana_pmu_query(settings),
            auth=auth,
        )
    finally:
        session.close()

    validate_grafana_datasource(
        datasource,
        GRAFANA_VICTORIA_METRICS_DATASOURCE_UID,
        GRAFANA_VICTORIA_METRICS_DATASOURCE_TYPE,
        "VictoriaMetrics",
    )
    validate_grafana_druid_datasource(druid_datasource)
    validate_grafana_dashboard(
        dashboard,
        GRAFANA_KAFKA_OPERATIONS_DASHBOARD_UID,
        "WAMA Infrastructure",
        "Kafka Operations",
    )
    validate_grafana_dashboard(
        pmu_dashboard,
        GRAFANA_PMU_MEASUREMENTS_DASHBOARD_UID,
        "WAMA Measurements",
        "PMU Live Measurements",
    )
    validate_grafana_pmu_dashboard(pmu_dashboard)
    validate_grafana_pmu_query(
        pmu_query,
        settings.druid_expected_mrid,
        settings.druid_expected_double_value,
    )


def validate_grafana_druid_datasource(payload: Any) -> None:
    """Require the Druid plugin datasource to use the internal Router URL."""

    validate_grafana_datasource(
        payload,
        GRAFANA_DRUID_DATASOURCE_UID,
        GRAFANA_DRUID_DATASOURCE_TYPE,
        "Druid",
    )
    json_data = payload.get("jsonData")
    if not isinstance(json_data, Mapping) or json_data.get("connection.url") != GRAFANA_DRUID_DATASOURCE_URL:
        raise ReadinessError("Grafana Druid datasource does not use the internal Router URL")


def _grafana_pmu_query(settings: Settings) -> dict[str, Any]:
    escaped_mrid = settings.druid_expected_mrid.replace("'", "''")
    return {
        "from": "now-15m",
        "to": "now",
        "queries": [
            {
                "refId": "A",
                "datasource": {
                    "uid": GRAFANA_DRUID_DATASOURCE_UID,
                    "type": GRAFANA_DRUID_DATASOURCE_TYPE,
                },
                "builder": {
                    "queryType": "sql",
                    "query": (
                        'SELECT "__time", "mrid", "double_value" '
                        f'FROM "{settings.druid_datasource}" '
                        f"WHERE \"mrid\" = '{escaped_mrid}' "
                        "AND \"quality_valid\" = 'true' "
                        'AND "double_value" IS NOT NULL '
                        'ORDER BY "__time" DESC LIMIT 1'
                    ),
                },
                "settings": {
                    "contextParameters": [],
                    "format": "long",
                },
                "intervalMs": 1_000,
                "maxDataPoints": 1_000,
            }
        ],
    }


def validate_grafana_pmu_query(
    payload: Any,
    expected_mrid: str,
    expected_double_value: float,
) -> None:
    """Require Grafana's Druid datasource to return the expected PMU frame."""

    if not isinstance(payload, Mapping):
        raise ReadinessError("Grafana Druid PMU query did not return an object")
    results = payload.get("results")
    if not isinstance(results, Mapping):
        raise ReadinessError("Grafana Druid PMU query has no results")
    result = results.get("A")
    if not isinstance(result, Mapping) or result.get("status") != 200:
        raise ReadinessError("Grafana Druid PMU query did not succeed")
    frames = result.get("frames")
    if not isinstance(frames, list) or not frames or not isinstance(frames[0], Mapping):
        raise ReadinessError("Grafana Druid PMU query returned no data frames")
    frame = frames[0]
    schema = frame.get("schema")
    data = frame.get("data")
    if not isinstance(schema, Mapping) or not isinstance(data, Mapping):
        raise ReadinessError("Grafana Druid PMU frame is invalid")
    fields = schema.get("fields")
    values = data.get("values")
    if not isinstance(fields, list) or not isinstance(values, list) or len(fields) != len(values):
        raise ReadinessError("Grafana Druid PMU frame has invalid fields")
    field_names = [field.get("name") if isinstance(field, Mapping) else None for field in fields]
    try:
        time_index = field_names.index("__time")
        mrid_index = field_names.index("mrid")
        value_index = field_names.index("double_value")
    except ValueError as error:
        raise ReadinessError("Grafana Druid PMU frame is missing required columns") from error
    time_values = values[time_index]
    mrid_values = values[mrid_index]
    measurement_values = values[value_index]
    if (
        not isinstance(time_values, list)
        or not isinstance(mrid_values, list)
        or not isinstance(measurement_values, list)
        or not time_values
        or len(time_values) != len(mrid_values)
        or len(time_values) != len(measurement_values)
    ):
        raise ReadinessError("Grafana Druid PMU frame has invalid row values")
    for timestamp, mrid, value in zip(time_values, mrid_values, measurement_values, strict=True):
        if isinstance(timestamp, bool) or not isinstance(timestamp, (int, float)):
            raise ReadinessError("Grafana Druid PMU frame has no numeric timestamp")
        if mrid != expected_mrid:
            continue
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ReadinessError("Grafana Druid PMU frame has no numeric double_value")
        if math.isclose(float(value), expected_double_value, abs_tol=1e-9):
            return
        raise ReadinessError("Grafana Druid PMU double_value does not match the PMU fixture")
    raise ReadinessError("Grafana Druid PMU frame did not return the expected MRID")


def validate_grafana_datasource(
    payload: Any,
    expected_uid: str,
    expected_type: str,
    name: str,
) -> None:
    """Require a Grafana datasource with the expected stable identity."""

    if not isinstance(payload, Mapping):
        raise ReadinessError(f"Grafana {name} datasource response is invalid")
    if payload.get("uid") != expected_uid or payload.get("type") != expected_type:
        raise ReadinessError(f"Grafana {name} datasource is not provisioned correctly")


def validate_grafana_dashboard(
    payload: Any,
    expected_uid: str,
    expected_folder: str,
    name: str,
) -> None:
    """Require a provisioned dashboard in its intended Grafana folder."""

    if not isinstance(payload, Mapping):
        raise ReadinessError(f"Grafana {name} dashboard response is invalid")
    metadata = payload.get("meta")
    dashboard_body = payload.get("dashboard")
    if (
        not isinstance(metadata, Mapping)
        or not metadata.get("provisioned")
        or not isinstance(dashboard_body, Mapping)
        or dashboard_body.get("uid") != expected_uid
        or metadata.get("folderTitle") != expected_folder
    ):
        raise ReadinessError(f"Grafana {name} dashboard is not provisioned correctly")


def validate_grafana_pmu_dashboard(payload: Any) -> None:
    """Require unit-safe Druid panels for the configured PMU measurement groups."""

    if not isinstance(payload, Mapping):
        raise ReadinessError("Grafana PMU dashboard response is invalid")
    dashboard = payload.get("dashboard")
    if not isinstance(dashboard, Mapping):
        raise ReadinessError("Grafana PMU dashboard has no body")
    panels = dashboard.get("panels")
    if not isinstance(panels, list):
        raise ReadinessError("Grafana PMU dashboard has no panels")
    expected_panels = {
        "Phase Voltages": ("timeseries", "volt"),
        "Phase Currents": ("timeseries", "amp"),
        "Frequency": ("timeseries", "hertz"),
        "ROCOF (Hz/s)": ("timeseries", "suffix:Hz/s"),
        "Latest Valid PMU Records": ("table", None),
    }
    by_title = {
        panel.get("title"): panel
        for panel in panels
        if isinstance(panel, Mapping) and isinstance(panel.get("title"), str)
    }
    for title, (expected_type, expected_unit) in expected_panels.items():
        panel = by_title.get(title)
        if not isinstance(panel, Mapping) or panel.get("type") != expected_type:
            raise ReadinessError(f"Grafana PMU dashboard is missing the {title!r} panel")
        datasource = panel.get("datasource")
        if (
            not isinstance(datasource, Mapping)
            or datasource.get("uid") != GRAFANA_DRUID_DATASOURCE_UID
            or datasource.get("type") != GRAFANA_DRUID_DATASOURCE_TYPE
        ):
            raise ReadinessError(f"Grafana PMU dashboard {title!r} does not use Druid")
        if expected_unit is None:
            continue
        field_config = panel.get("fieldConfig")
        defaults = field_config.get("defaults") if isinstance(field_config, Mapping) else None
        if not isinstance(defaults, Mapping) or defaults.get("unit") != expected_unit:
            raise ReadinessError(f"Grafana PMU dashboard {title!r} has an unexpected unit")


def check_victoria_metrics(settings: Settings) -> None:
    """Require all declared monitoring targets and Kafka topic metadata metrics."""

    session = requests.Session()
    try:
        for job in MONITORING_JOBS:
            payload = _prometheus_query(session, settings.victoria_metrics_url, f'up{{job="{job}"}}')
            validate_prometheus_up(payload, job)
        broker_payload = _prometheus_query(
            session,
            settings.victoria_metrics_url,
            'max(kafka_brokers{job="kafka-exporter"})',
        )
        if _single_prometheus_value(broker_payload, "Kafka broker count") < 1:
            raise ReadinessError("Kafka exporter did not report a broker")
        topic_payload = _prometheus_query(
            session,
            settings.victoria_metrics_url,
            'count(kafka_topic_partitions{job="kafka-exporter"})',
        )
    finally:
        session.close()

    topic_count = _single_prometheus_value(topic_payload, "Kafka topic count")
    if topic_count != len(REQUIRED_TOPICS):
        raise ReadinessError(
            f"Kafka exporter must report {len(REQUIRED_TOPICS)} WAMA topics; found {topic_count:g}"
        )


def _prometheus_query(session: requests.Session, base_url: str, query: str) -> Any:
    return _request_json(
        session,
        f"VictoriaMetrics query {query}",
        _url(base_url, "/api/v1/query"),
        params={"query": query},
    )


def validate_prometheus_up(payload: Any, job: str) -> None:
    """Require every matching Prometheus `up` sample to equal one."""

    values = _prometheus_vector(payload, f"monitoring job {job}")
    if not values:
        raise ReadinessError(f"VictoriaMetrics has no up metric for job {job}")
    if any(value != 1 for value in values):
        raise ReadinessError(f"VictoriaMetrics reports monitoring job {job} as down")


def _single_prometheus_value(payload: Any, name: str) -> float:
    values = _prometheus_vector(payload, name)
    if len(values) != 1:
        raise ReadinessError(f"VictoriaMetrics returned {len(values)} samples for {name}")
    return values[0]


def _prometheus_vector(payload: Any, name: str) -> list[float]:
    if not isinstance(payload, Mapping) or payload.get("status") != "success":
        raise ReadinessError(f"VictoriaMetrics did not return a successful response for {name}")
    data = payload.get("data")
    if not isinstance(data, Mapping) or data.get("resultType") != "vector":
        raise ReadinessError(f"VictoriaMetrics did not return a vector for {name}")
    result = data.get("result")
    if not isinstance(result, list):
        raise ReadinessError(f"VictoriaMetrics did not return samples for {name}")
    values: list[float] = []
    for sample in result:
        if not isinstance(sample, Mapping):
            raise ReadinessError(f"VictoriaMetrics returned an invalid sample for {name}")
        value = sample.get("value")
        if not isinstance(value, Sequence) or len(value) != 2:
            raise ReadinessError(f"VictoriaMetrics returned an invalid value for {name}")
        try:
            values.append(float(value[1]))
        except (TypeError, ValueError) as error:
            raise ReadinessError(f"VictoriaMetrics returned a non-numeric value for {name}") from error
    return values


def _request_json(
    session: requests.Session,
    label: str,
    url: str,
    *,
    auth: tuple[str, str] | None = None,
    params: Mapping[str, str] | None = None,
) -> Any:
    try:
        response = session.get(url, auth=auth, params=params, timeout=5)
        response.raise_for_status()
    except requests.RequestException as error:
        raise ReadinessError(f"{label} request failed: {error}") from error
    try:
        return response.json()
    except ValueError as error:
        raise ReadinessError(f"{label} did not return JSON") from error


def _post_json(
    session: requests.Session,
    label: str,
    url: str,
    payload: Mapping[str, Any],
    *,
    auth: tuple[str, str] | None = None,
) -> Any:
    try:
        response = session.post(url, json=payload, auth=auth, timeout=10)
        response.raise_for_status()
    except requests.RequestException as error:
        raise ReadinessError(f"{label} request failed: {error}") from error
    try:
        return response.json()
    except ValueError as error:
        raise ReadinessError(f"{label} did not return JSON") from error


def _url(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}{path}"