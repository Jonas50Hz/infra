"""Prove complete and partial MeasurementSession requests through Blobmeta."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import os
from threading import Event
from typing import Any
from uuid import UUID, uuid4

import boto3
from botocore.config import Config
from google.protobuf.message import DecodeError
from google.protobuf.timestamp_pb2 import Timestamp
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from measurement_session_common.contract import (
    DEFAULT_SESSION_BUCKET,
    SESSION_PARQUET_SCHEMA_VERSION,
    ContractValidationError,
    validate_blobmeta,
    validate_kafka_key,
    validate_measurement_session_request,
)
from measurement_session_common.generated.blobmeta_pb2 import Blobmeta
from measurement_session_common.generated.measurement_session_pb2 import MeasurementSessionRequest


class RequestFlowError(RuntimeError):
    """Raised when the request-to-Blobmeta evidence chain cannot be proven."""


@dataclass(frozen=True)
class Settings:
    """Test-only endpoints and stable request identities."""

    blobmeta_topic: str
    complete_session_id: str
    grafana_password: str
    grafana_url: str
    grafana_username: str
    kafka_bootstrap_servers: str
    kafka_consume_timeout_seconds: int
    missing_mrid: str
    partial_session_id: str
    s3_access_key_id: str
    s3_bucket: str
    s3_endpoint_url: str
    s3_region: str
    s3_secret_access_key: str
    session_topic: str
    test_mrid: str
    timeout_seconds: int
    trino_druid_catalog: str
    trino_druid_schema: str
    trino_druid_table: str
    trino_blobmeta_catalog: str
    trino_blobmeta_schema: str
    trino_query_index_schema: str
    trino_query_index_table: str
    trino_url: str
    trino_user: str

    @classmethod
    def from_environment(cls, environment: dict[str, str] | None = None) -> "Settings":
        """Load all e2e inputs and reject ambiguous session identities early."""

        values = os.environ if environment is None else environment
        complete_session_id = _uuid(values, "MEASUREMENT_SESSION_COMPLETE_ID")
        partial_session_id = _uuid(values, "MEASUREMENT_SESSION_PARTIAL_ID")
        if complete_session_id == partial_session_id:
            raise RequestFlowError("MeasurementSession e2e IDs must be distinct")
        bucket = _required(values, "S3_BUCKET", DEFAULT_SESSION_BUCKET)
        if bucket != DEFAULT_SESSION_BUCKET:
            raise RequestFlowError(f"S3_BUCKET must be {DEFAULT_SESSION_BUCKET!r}")
        return cls(
            blobmeta_topic=_required(values, "BLOBMETA_TOPIC", "Blobmeta"),
            complete_session_id=complete_session_id,
            grafana_password=_required(values, "GF_SECURITY_ADMIN_PASSWORD", "wama-admin"),
            grafana_url=_url(values, "GRAFANA_URL", "http://grafana:3000"),
            grafana_username=_required(values, "GF_SECURITY_ADMIN_USER", "wama-admin"),
            kafka_bootstrap_servers=_required(values, "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            kafka_consume_timeout_seconds=_positive_integer(
                values,
                "KAFKA_CONSUME_TIMEOUT_SECONDS",
                30,
            ),
            missing_mrid=_required(
                values,
                "MEASUREMENT_SESSION_TEST_MISSING_MRID",
                "urn:wama:poc:missing",
            ),
            partial_session_id=partial_session_id,
            s3_access_key_id=_required(values, "S3_ACCESS_KEY_ID", "wama-s3-admin"),
            s3_bucket=bucket,
            s3_endpoint_url=_url(values, "S3_ENDPOINT_URL", "http://seaweedfs:8333"),
            s3_region=_required(values, "S3_REGION", "us-east-1"),
            s3_secret_access_key=_required(
                values,
                "S3_SECRET_ACCESS_KEY",
                "wama-s3-admin-secret",
            ),
            session_topic=_required(values, "MEASUREMENT_SESSION_TOPIC", "MeasurementSession"),
            test_mrid=_required(
                values,
                "MEASUREMENT_SESSION_TEST_MRID",
                "urn:wama:poc:pmu:bay-01:frequency",
            ),
            timeout_seconds=_positive_integer(values, "TIMEOUT_SECONDS", 60),
            trino_druid_catalog=_identifier(values, "TRINO_DRUID_CATALOG", "druid"),
            trino_druid_schema=_identifier(values, "TRINO_DRUID_SCHEMA", "druid"),
            trino_druid_table=_identifier(
                values,
                "TRINO_DRUID_TABLE",
                "live_measurements",
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
            trino_query_index_schema=_identifier(
                values,
                "TRINO_QUERY_INDEX_SCHEMA",
                "session_query_index",
            ),
            trino_query_index_table=_identifier(
                values,
                "TRINO_QUERY_INDEX_TABLE",
                "registrations",
            ),
            trino_url=_url(values, "TRINO_URL", "http://trino:8080"),
            trino_user=_required(values, "TRINO_USER", "measurement-session-e2e"),
        )


def main() -> None:
    """Create two independent requests and prove their durable materialization."""

    try:
        settings = Settings.from_environment()
        anchor = latest_measurement_timestamp(settings)
        requested_at = datetime.now(timezone.utc)
        complete_request = build_request(
            settings.complete_session_id,
            requested_at,
            anchor - timedelta(seconds=1),
            anchor + timedelta(seconds=1),
            (settings.test_mrid,),
        )
        partial_request = build_request(
            settings.partial_session_id,
            requested_at,
            anchor - timedelta(seconds=1),
            anchor + timedelta(seconds=1),
            tuple(sorted((settings.test_mrid, settings.missing_mrid))),
        )
        results = publish_and_consume(settings, (complete_request, partial_request))
        complete = results[settings.complete_session_id]
        partial = results[settings.partial_session_id]
        if complete.status != Blobmeta.COMPLETE:
            raise RequestFlowError("Complete request did not produce COMPLETE Blobmeta")
        if partial.status != Blobmeta.PARTIAL:
            raise RequestFlowError("Partial request did not produce PARTIAL Blobmeta")
        wait_for_catalog(settings, (complete, partial))
        verify_artifacts(settings, (complete, partial))
        wait_for_query_index(settings, (complete, partial))
    except (
        ContractValidationError,
        DecodeError,
        KafkaError,
        OSError,
        RequestFlowError,
        ValueError,
        requests.RequestException,
    ) as error:
        raise SystemExit(f"MeasurementSession request-to-Blobmeta validation failed: {error}") from error

    print(
        "MeasurementSession request-to-Blobmeta validation passed for "
        f"{settings.complete_session_id} and {settings.partial_session_id}."
    )


def build_request(
    session_id: str,
    requested_at: datetime,
    started_at: datetime,
    ended_at: datetime,
    mrids: tuple[str, ...],
) -> MeasurementSessionRequest:
    """Build and validate one raw-Protobuf request exactly as a producer must."""

    request = MeasurementSessionRequest(session_id=session_id, mrids=mrids)
    _set_timestamp(request.requested_at, requested_at)
    _set_timestamp(request.started_at, started_at)
    _set_timestamp(request.ended_at, ended_at)
    metadata = request.metadata.add()
    metadata.key = "capture_reason"
    metadata.value = "e2e"
    validate_measurement_session_request(request)
    return request


def latest_measurement_timestamp(settings: Settings) -> datetime:
    """Find one Trino-visible Druid value to ensure the complete request has data."""

    escaped_mrid = settings.test_mrid.replace("'", "''")
    rows = _execute_trino(
        settings,
        'SELECT CAST("__time" AS VARCHAR) '
        f"FROM {settings.trino_druid_catalog}.{settings.trino_druid_schema}."
        f"{settings.trino_druid_table} "
        f"WHERE mrid = '{escaped_mrid}' "
        'ORDER BY "__time" DESC LIMIT 1',
    )
    if len(rows) != 1 or len(rows[0]) != 1:
        raise RequestFlowError("Trino Druid catalog has no queryable test measurement")
    return _parse_timestamp(rows[0][0], "Trino Druid measurement timestamp")


def publish_and_consume(
    settings: Settings,
    requests_to_publish: tuple[MeasurementSessionRequest, ...],
) -> dict[str, Blobmeta]:
    """Publish input commands and consume only their matching compacted results."""

    expected_ids = {request.session_id for request in requests_to_publish}
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
        acks="all",
        retries=5,
        request_timeout_ms=30_000,
    )
    consumer = KafkaConsumer(
        settings.blobmeta_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
        client_id="measurement-session-e2e",
        group_id=f"measurement-session-e2e-{uuid4().hex}",
        enable_auto_commit=False,
        auto_offset_reset="latest",
        request_timeout_ms=30_000,
        api_version_auto_timeout_ms=10_000,
    )
    try:
        consumer.poll(timeout_ms=1_000)
        for request in requests_to_publish:
            payload = request.SerializeToString(deterministic=True)
            timestamp_milliseconds = (
                request.requested_at.seconds * 1_000 + request.requested_at.nanos // 1_000_000
            )
            producer.send(
                settings.session_topic,
                key=request.session_id.encode("utf-8"),
                value=payload,
                timestamp_ms=timestamp_milliseconds,
            ).get(timeout=30)
        deadline = _deadline(settings.timeout_seconds)
        results: dict[str, Blobmeta] = {}
        while not _expired(deadline):
            records = consumer.poll(timeout_ms=1_000)
            for partition_records in records.values():
                for record in partition_records:
                    result = validate_blobmeta_record(record)
                    if result.session_id in expected_ids:
                        results[result.session_id] = result
            if set(results) == expected_ids:
                return results
    finally:
        consumer.close(autocommit=False)
        producer.close(timeout=30)
    raise RequestFlowError("Kafka did not return every expected Blobmeta result")


def validate_blobmeta_record(record: Any) -> Blobmeta:
    """Require key, wire payload, and record timestamp to match Blobmeta evidence."""

    result = Blobmeta()
    try:
        result.ParseFromString(record.value)
    except DecodeError as error:
        raise RequestFlowError("Blobmeta Kafka value is not raw Protobuf") from error
    validate_blobmeta(result)
    validate_kafka_key(record.key, result.blob_id, "blob_id")
    expected_timestamp = result.finalized_at.seconds * 1_000 + result.finalized_at.nanos // 1_000_000
    if record.timestamp != expected_timestamp:
        raise RequestFlowError("Blobmeta Kafka timestamp does not match finalized_at")
    return result


def wait_for_catalog(settings: Settings, results: tuple[Blobmeta, ...]) -> None:
    """Require every Kafka result to reach its metadata projection through Trino."""

    expected = {result.blob_id: result for result in results}
    deadline = _deadline(settings.timeout_seconds)
    while not _expired(deadline):
        try:
            blob_rows = _execute_trino(settings, _blobmeta_rows_query(settings, expected))
            validate_catalog_rows(_catalog_rows(blob_rows), expected)
            coverage_rows = _execute_trino(settings, _blobmeta_coverage_query(settings, expected))
            validate_catalog_coverage(_catalog_coverage(coverage_rows), expected)
            return
        except (RequestFlowError, ValueError, requests.RequestException):
            pass
        Event().wait(0.5)
    raise RequestFlowError("Blobmeta catalog did not materialize through Trino before timeout")


def verify_artifacts(settings: Settings, results: tuple[Blobmeta, ...]) -> None:
    """Check SeaweedFS metadata, hashes, Parquet rows, and coverage counts."""

    client = boto3.client(
        "s3",
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        endpoint_url=settings.s3_endpoint_url,
        region_name=settings.s3_region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    for result in results:
        if not result.HasField("object"):
            raise RequestFlowError("Completed Blobmeta has no Parquet object")
        reference = result.object
        if reference.parquet_schema_version != SESSION_PARQUET_SCHEMA_VERSION:
            raise RequestFlowError("Blobmeta has an unsupported Parquet schema version")
        head = client.head_object(Bucket=reference.bucket, Key=reference.object_key)
        response = client.get_object(Bucket=reference.bucket, Key=reference.object_key)
        body = response["Body"]
        try:
            payload = body.read()
        finally:
            body.close()
        if (
            head.get("ContentLength") != reference.byte_length
            or head.get("Metadata", {}).get("sha256") != bytes(reference.sha256).hex()
            or sha256(payload).digest() != bytes(reference.sha256)
        ):
            raise RequestFlowError("SeaweedFS object integrity does not match Blobmeta")
        table = pq.read_table(pa.BufferReader(payload))
        if table.schema.metadata.get(b"wama.parquet.schema_version") != str(
            SESSION_PARQUET_SCHEMA_VERSION
        ).encode("ascii"):
            raise RequestFlowError("Parquet schema version does not match Blobmeta")
        if table.num_rows != result.measurement_count:
            raise RequestFlowError("Parquet row count does not match Blobmeta")
        if table.column("blob_id").to_pylist() != [result.blob_id] * table.num_rows:
            raise RequestFlowError("Parquet blob_id values do not match Blobmeta")
        if table.column("session_id").to_pylist() != [result.session_id] * table.num_rows:
            raise RequestFlowError("Parquet session_id values do not match Blobmeta")
        expected_coverage = {
            coverage.mrid: coverage.measurement_count
            for coverage in result.mrid_coverage
        }
        actual_coverage = _normalized_parquet_coverage(
            table.column("mrid").to_pylist(),
            expected_coverage,
        )
        if actual_coverage != expected_coverage:
            raise RequestFlowError("Parquet MRID coverage does not match Blobmeta")


def wait_for_query_index(settings: Settings, results: tuple[Blobmeta, ...]) -> None:
    """Require every canonical artifact to reach the ledger and public Iceberg reader."""

    expected = {
        result.blob_id: (result.session_id, result.measurement_count)
        for result in results
    }
    deadline = _deadline(settings.timeout_seconds)
    while not _expired(deadline):
        try:
            _verify_query_ledger(settings, results)
            query_rows = _query_indexed_rows(settings, expected)
            validate_query_index_rows(query_rows, expected)
            _verify_grafana_session_query(settings, results[0])
            return
        except (RequestFlowError, ValueError, requests.RequestException):
            Event().wait(0.5)
    raise RequestFlowError("MeasurementSession artifacts did not reach the query index")


def validate_query_index_rows(
    actual: dict[str, tuple[str, int]],
    expected: dict[str, tuple[str, int]],
) -> None:
    """Require public Iceberg rows to preserve Blobmeta identity and count evidence."""

    if actual != expected:
        raise RequestFlowError("Iceberg query rows do not match Blobmeta evidence")


def _verify_query_ledger(settings: Settings, results: tuple[Blobmeta, ...]) -> None:
    expected = {
        result.blob_id: (
            result.session_id,
            f"s3://{result.object.bucket}/{result.object.object_key}",
            bytes(result.object.sha256).hex(),
            result.object.byte_length,
            result.measurement_count,
        )
        for result in results
    }
    rows = _execute_trino(settings, _query_index_ledger_query(settings, expected))
    validate_query_ledger_rows(_query_ledger_rows(rows), expected)


def _query_indexed_rows(
    settings: Settings,
    expected: dict[str, tuple[str, int]],
) -> dict[str, tuple[str, int]]:
    blob_ids = ", ".join(_sql_literal(blob_id) for blob_id in expected)
    rows = _execute_trino(
        settings,
        "SELECT blob_id, session_id, count(*) "
        "FROM sessions.wama.measurement_values "
        f"WHERE blob_id IN ({blob_ids}) "
        "GROUP BY blob_id, session_id",
    )
    actual: dict[str, tuple[str, int]] = {}
    for row in rows:
        if (
            len(row) != 3
            or not isinstance(row[0], str)
            or not isinstance(row[1], str)
            or isinstance(row[2], bool)
            or not isinstance(row[2], int)
        ):
            raise RequestFlowError("Iceberg query returned an invalid row")
        actual[row[0]] = (row[1], row[2])
    return actual


def validate_catalog_rows(
    actual: dict[str, tuple[str, int, int]],
    expected: dict[str, Blobmeta],
) -> None:
    """Require Trino Blobmeta rows to preserve immutable result evidence."""

    expected_rows = {
        blob_id: (
            Blobmeta.Status.Name(result.status),
            result.measurement_count,
            result.object.parquet_schema_version,
        )
        for blob_id, result in expected.items()
    }
    if actual != expected_rows:
        raise RequestFlowError("Trino Blobmeta rows do not match immutable evidence")


def validate_catalog_coverage(
    actual: dict[str, dict[str, int]],
    expected: dict[str, Blobmeta],
) -> None:
    """Require Trino Blobmeta coverage rows to preserve all MRID counts."""

    expected_coverage = {
        blob_id: {
            coverage.mrid: coverage.measurement_count
            for coverage in result.mrid_coverage
        }
        for blob_id, result in expected.items()
    }
    if actual != expected_coverage:
        raise RequestFlowError("Trino Blobmeta MRID coverage does not match immutable evidence")


def validate_query_ledger_rows(
    actual: dict[str, tuple[str, str, str, int, int]],
    expected: dict[str, tuple[str, str, str, int, int]],
) -> None:
    """Require Trino query-index ledger rows to preserve Blobmeta identity."""

    if actual != expected:
        raise RequestFlowError("Trino query index ledger does not match Blobmeta evidence")


def _blobmeta_rows_query(settings: Settings, expected: dict[str, Blobmeta]) -> str:
    blob_ids = _sql_literals(expected)
    return (
        "SELECT blob_id, status, measurement_count, object_parquet_schema_version "
        f"FROM {settings.trino_blobmeta_catalog}.{settings.trino_blobmeta_schema}.session_blobs "
        f"WHERE blob_id IN ({blob_ids})"
    )


def _blobmeta_coverage_query(settings: Settings, expected: dict[str, Blobmeta]) -> str:
    blob_ids = _sql_literals(expected)
    return (
        "SELECT blob_id, mrid, measurement_count "
        f"FROM {settings.trino_blobmeta_catalog}.{settings.trino_blobmeta_schema}.session_blob_mrids "
        f"WHERE blob_id IN ({blob_ids})"
    )


def _query_index_ledger_query(
    settings: Settings,
    expected: dict[str, tuple[str, str, str, int, int]],
) -> str:
    blob_ids = _sql_literals(expected)
    return (
        "SELECT blob_id, CAST(session_id AS VARCHAR), object_uri, to_hex(object_sha256), "
        "object_byte_length, measurement_count "
        f"FROM {settings.trino_blobmeta_catalog}.{settings.trino_query_index_schema}."
        f"{settings.trino_query_index_table} "
        f"WHERE blob_id IN ({blob_ids})"
    )


def _catalog_rows(rows: list[tuple[Any, ...]]) -> dict[str, tuple[str, int, int]]:
    actual: dict[str, tuple[str, int, int]] = {}
    for row in rows:
        if (
            len(row) != 4
            or not isinstance(row[0], str)
            or not isinstance(row[1], str)
            or isinstance(row[2], bool)
            or not isinstance(row[2], int)
            or isinstance(row[3], bool)
            or not isinstance(row[3], int)
            or row[0] in actual
        ):
            raise RequestFlowError("Trino Blobmeta rows have an invalid shape")
        actual[row[0]] = (row[1], row[2], row[3])
    return actual


def _catalog_coverage(rows: list[tuple[Any, ...]]) -> dict[str, dict[str, int]]:
    actual: dict[str, dict[str, int]] = {}
    for row in rows:
        if (
            len(row) != 3
            or not isinstance(row[0], str)
            or not isinstance(row[1], str)
            or isinstance(row[2], bool)
            or not isinstance(row[2], int)
            or row[1] in actual.get(row[0], {})
        ):
            raise RequestFlowError("Trino Blobmeta coverage rows have an invalid shape")
        actual.setdefault(row[0], {})[row[1]] = row[2]
    return actual


def _query_ledger_rows(rows: list[tuple[Any, ...]]) -> dict[str, tuple[str, str, str, int, int]]:
    actual: dict[str, tuple[str, str, str, int, int]] = {}
    for row in rows:
        if (
            len(row) != 6
            or not all(isinstance(value, str) for value in row[:4])
            or isinstance(row[4], bool)
            or not isinstance(row[4], int)
            or isinstance(row[5], bool)
            or not isinstance(row[5], int)
            or row[0] in actual
        ):
            raise RequestFlowError("Trino query index ledger rows have an invalid shape")
        actual[row[0]] = (row[1], row[2], row[3].lower(), row[4], row[5])
    return actual


def _execute_trino(settings: Settings, statement: str) -> list[tuple[Any, ...]]:
    response = requests.post(
        f"{settings.trino_url}/v1/statement",
        data=statement,
        headers={"X-Trino-User": settings.trino_user},
        timeout=10,
    )
    response.raise_for_status()
    payload: Any = response.json()
    rows: list[tuple[Any, ...]] = []
    for _ in range(100):
        if not isinstance(payload, Mapping):
            raise RequestFlowError("Trino statement response is not an object")
        error = payload.get("error")
        if error is not None:
            raise RequestFlowError(f"Trino statement failed: {error}")
        values = payload.get("data")
        if values is not None:
            if not isinstance(values, list) or any(not isinstance(row, list) for row in values):
                raise RequestFlowError("Trino statement response has invalid rows")
            rows.extend(tuple(row) for row in values)
        next_uri = payload.get("nextUri")
        if next_uri is None:
            return rows
        if not isinstance(next_uri, str) or not next_uri.startswith(("http://", "https://")):
            raise RequestFlowError("Trino statement response has an invalid nextUri")
        response = requests.get(next_uri, headers={"X-Trino-User": settings.trino_user}, timeout=30)
        response.raise_for_status()
        payload = response.json()
    raise RequestFlowError("Trino statement exceeded the supported result page limit")


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sql_literals(values: Mapping[str, object]) -> str:
    return ", ".join(_sql_literal(value) for value in values)


def _verify_grafana_session_query(settings: Settings, result: Blobmeta) -> None:
    response = requests.post(
        f"{settings.grafana_url}/api/ds/query",
        json={
            "from": "now-24h",
            "to": "now",
            "queries": [
                {
                    "refId": "A",
                    "datasource": {"uid": "trino", "type": "trino-datasource"},
                    "rawSQL": (
                        "SELECT timestamp_mccs AS time, double_value AS value, mrid AS metric "
                        "FROM sessions.wama.measurement_values "
                        f"WHERE blob_id = {_sql_literal(result.blob_id)} "
                        "AND double_value IS NOT NULL "
                        "AND $__timeFilter(timestamp_mccs) ORDER BY time ASC"
                    ),
                    "format": 0,
                    "intervalMs": 1_000,
                    "maxDataPoints": 1_000,
                }
            ],
        },
        auth=(settings.grafana_username, settings.grafana_password),
        timeout=15,
    )
    response.raise_for_status()
    validate_grafana_session_query(response.json())


def validate_grafana_session_query(payload: Any) -> None:
    """Require Grafana's Trino datasource to return one selected session frame."""

    if not isinstance(payload, Mapping):
        raise RequestFlowError("Grafana session query did not return an object")
    results = payload.get("results")
    if not isinstance(results, Mapping):
        raise RequestFlowError("Grafana session query has no results")
    result = results.get("A")
    if not isinstance(result, Mapping) or result.get("status") != 200:
        raise RequestFlowError("Grafana session query did not succeed")
    frames = result.get("frames")
    if not isinstance(frames, list) or not frames or not isinstance(frames[0], Mapping):
        raise RequestFlowError("Grafana session query returned no data frames")
    frame = frames[0]
    schema = frame.get("schema")
    data = frame.get("data")
    if not isinstance(schema, Mapping) or not isinstance(data, Mapping):
        raise RequestFlowError("Grafana session query frame is invalid")
    fields = schema.get("fields")
    values = data.get("values")
    if not isinstance(fields, list) or not isinstance(values, list) or len(fields) != len(values):
        raise RequestFlowError("Grafana session query frame has invalid fields")
    field_names = [field.get("name") if isinstance(field, Mapping) else None for field in fields]
    try:
        time_index = field_names.index("time")
        value_index = field_names.index("value")
    except ValueError as error:
        raise RequestFlowError("Grafana session query frame is missing required columns") from error
    time_values = values[time_index]
    measurement_values = values[value_index]
    if (
        not isinstance(time_values, list)
        or not isinstance(measurement_values, list)
        or not time_values
        or len(time_values) != len(measurement_values)
    ):
        raise RequestFlowError("Grafana session query frame has no aligned values")
    if "metric" in field_names:
        metric_values = values[field_names.index("metric")]
        if not isinstance(metric_values, list) or len(metric_values) != len(time_values):
            raise RequestFlowError("Grafana session query frame has invalid metric values")
        return
    value_field = fields[value_index]
    labels = value_field.get("labels") if isinstance(value_field, Mapping) else None
    if not isinstance(labels, Mapping) or not isinstance(labels.get("metric"), str):
        raise RequestFlowError("Grafana session query frame has no metric label")


def _normalized_parquet_coverage(
    mrids: list[str],
    expected_coverage: dict[str, int],
) -> dict[str, int]:
    """Represent requested MRIDs absent from a partial Parquet file as zero."""

    actual_counts = Counter(mrids)
    if unexpected := set(actual_counts).difference(expected_coverage):
        raise RequestFlowError(
            f"Parquet contains MRIDs not present in Blobmeta: {', '.join(sorted(unexpected))}"
        )
    return {mrid: actual_counts[mrid] for mrid in expected_coverage}


def _uuid(values: dict[str, str], name: str) -> str:
    value = _required(values, name)
    try:
        parsed = UUID(value)
    except ValueError as error:
        raise RequestFlowError(f"{name} must be a lowercase canonical UUID") from error
    if str(parsed) != value:
        raise RequestFlowError(f"{name} must be a lowercase canonical UUID")
    return value


def _required(values: dict[str, str], name: str, default: str | None = None) -> str:
    value = values.get(name, default or "").strip()
    if not value:
        raise RequestFlowError(f"{name} must not be empty")
    return value


def _identifier(values: dict[str, str], name: str, default: str) -> str:
    value = _required(values, name, default)
    if not value.replace("_", "").isalnum():
        raise RequestFlowError(f"{name} must contain only letters, numbers, and underscores")
    return value


def _url(values: dict[str, str], name: str, default: str) -> str:
    value = _required(values, name, default).rstrip("/")
    if not value.startswith(("http://", "https://")):
        raise RequestFlowError(f"{name} must be an HTTP URL")
    return value


def _positive_integer(values: dict[str, str], name: str, default: int) -> int:
    try:
        value = int(values.get(name, str(default)))
    except ValueError as error:
        raise RequestFlowError(f"{name} must be an integer") from error
    if value <= 0:
        raise RequestFlowError(f"{name} must be greater than zero")
    return value


def _set_timestamp(destination: Timestamp, value: datetime) -> None:
    destination.FromDatetime(value.astimezone(timezone.utc))


def _parse_timestamp(value: Any, location: str) -> datetime:
    if not isinstance(value, str):
        raise RequestFlowError(f"{location} is missing")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise RequestFlowError(f"{location} is malformed") from error
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _deadline(timeout_seconds: int) -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=timeout_seconds)


def _expired(deadline: datetime) -> bool:
    return datetime.now(timezone.utc) >= deadline


if __name__ == "__main__":
    main()