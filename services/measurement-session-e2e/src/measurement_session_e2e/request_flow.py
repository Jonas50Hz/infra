"""Prove complete and partial MeasurementSession requests through Blobmeta."""

from __future__ import annotations

from collections import Counter
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
import psycopg
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from measurement_session_common.contract import (
    DEFAULT_SESSION_BUCKET,
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
    druid_datasource: str
    druid_router_url: str
    kafka_bootstrap_servers: str
    kafka_consume_timeout_seconds: int
    missing_mrid: str
    partial_session_id: str
    postgres_dsn: str
    s3_access_key_id: str
    s3_bucket: str
    s3_endpoint_url: str
    s3_region: str
    s3_secret_access_key: str
    session_topic: str
    test_mrid: str
    timeout_seconds: int

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
            druid_datasource=_identifier(values, "DRUID_DATASOURCE", "live_measurements"),
            druid_router_url=_url(values, "DRUID_ROUTER_URL", "http://druid:8888"),
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
            session_topic=_required(values, "MEASUREMENT_SESSION_TOPIC", "MeasurementSession"),
            test_mrid=_required(
                values,
                "MEASUREMENT_SESSION_TEST_MRID",
                "urn:wama:poc:pmu:bay-01:frequency",
            ),
            timeout_seconds=_positive_integer(values, "TIMEOUT_SECONDS", 60),
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
    except (
        ContractValidationError,
        DecodeError,
        KafkaError,
        OSError,
        psycopg.Error,
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
    """Find one Druid-visible value to ensure the complete request has data."""

    escaped_mrid = settings.test_mrid.replace("'", "''")
    response = requests.post(
        f"{settings.druid_router_url}/druid/v2/sql",
        json={
            "query": (
                'SELECT "__time" FROM '
                f'"{settings.druid_datasource}" '
                f"WHERE \"mrid\" = '{escaped_mrid}' "
                'ORDER BY "__time" DESC LIMIT 1'
            ),
            "resultFormat": "object",
        },
        timeout=10,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list) or not payload or not isinstance(payload[0], dict):
        raise RequestFlowError("Druid has no queryable test measurement")
    return _parse_timestamp(payload[0].get("__time"), "Druid measurement timestamp")


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
    """Require every Kafka result to reach its metadata-only PostgreSQL projection."""

    expected = {result.blob_id: result for result in results}
    deadline = _deadline(settings.timeout_seconds)
    while not _expired(deadline):
        try:
            with psycopg.connect(settings.postgres_dsn, connect_timeout=5) as connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        SELECT blob_id, status, measurement_count
                        FROM blobmeta_catalog.session_blobs
                        WHERE blob_id = ANY(%s);
                        """,
                        (list(expected),),
                    )
                    rows = {
                        str(blob_id): (str(status), int(count))
                        for blob_id, status, count in cursor.fetchall()
                    }
                    if len(rows) == len(expected) and all(
                        rows[blob_id] == (
                            Blobmeta.Status.Name(result.status),
                            result.measurement_count,
                        )
                        for blob_id, result in expected.items()
                    ):
                        _verify_catalog_coverage(cursor, expected)
                        return
        except psycopg.Error:
            pass
        Event().wait(0.5)
    raise RequestFlowError("Blobmeta catalog did not materialize before timeout")


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
        if table.num_rows != result.measurement_count:
            raise RequestFlowError("Parquet row count does not match Blobmeta")
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


def _verify_catalog_coverage(cursor: Any, expected: dict[str, Blobmeta]) -> None:
    cursor.execute(
        """
        SELECT blob_id, mrid, measurement_count
        FROM blobmeta_catalog.session_blob_mrids
        WHERE blob_id = ANY(%s);
        """,
        (list(expected),),
    )
    actual: dict[str, dict[str, int]] = {}
    for blob_id, mrid, count in cursor.fetchall():
        actual.setdefault(str(blob_id), {})[str(mrid)] = int(count)
    for blob_id, result in expected.items():
        expected_coverage = {
            coverage.mrid: coverage.measurement_count
            for coverage in result.mrid_coverage
        }
        if actual.get(blob_id, {}) != expected_coverage:
            raise RequestFlowError("PostgreSQL MRID coverage does not match Blobmeta")


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
        raise RequestFlowError(f"{location} has no timezone")
    return parsed.astimezone(timezone.utc)


def _deadline(timeout_seconds: int) -> datetime:
    return datetime.now(timezone.utc) + timedelta(seconds=timeout_seconds)


def _expired(deadline: datetime) -> bool:
    return datetime.now(timezone.utc) >= deadline


if __name__ == "__main__":
    main()