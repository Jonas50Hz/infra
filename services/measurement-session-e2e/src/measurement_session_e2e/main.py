"""Verify a finalized raw-Protobuf session through its browser download path."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from io import StringIO
import os
from pathlib import Path
import re
import time
from typing import Any
from uuid import UUID, uuid4

from google.protobuf.message import DecodeError
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import requests

from measurement_session_common.contract import ContractValidationError, validate_measurement_session
from measurement_session_common.generated.measurement_session_pb2 import MeasurementSession
from measurement_session_common.measurement_csv import (
    MeasurementSeriesValidationError,
    validate_measurement_csv as validate_measurement_csv_stream,
)


class ContractToDownloadError(RuntimeError):
    """Raised when a finalized session cannot be proven from contract to download."""


@dataclass(frozen=True)
class Settings:
    """Minimal test-only inputs with no database or object-store credentials."""

    browser_url: str
    http_timeout_seconds: float
    kafka_bootstrap_servers: str
    kafka_consume_timeout_seconds: int
    kafka_topic: str
    session_id: str

    @classmethod
    def from_environment(cls, environment: dict[str, str] | None = None) -> "Settings":
        """Load strict test inputs from the environment."""

        values = os.environ if environment is None else environment
        session_id = _required(values, "MEASUREMENT_SESSION_ID")
        if str(UUID(session_id)) != session_id:
            raise ContractToDownloadError("MEASUREMENT_SESSION_ID must be a lowercase canonical UUID")
        return cls(
            browser_url=_url(values, "MEASUREMENT_SESSION_BROWSER_URL", "http://measurement-session-browser:8080"),
            http_timeout_seconds=_positive_float(values, "HTTP_TIMEOUT_SECONDS", 45),
            kafka_bootstrap_servers=_required(values, "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            kafka_consume_timeout_seconds=_positive_integer(values, "KAFKA_CONSUME_TIMEOUT_SECONDS", 30),
            kafka_topic=_required(values, "KAFKA_TOPIC", "MeasurementSession"),
            session_id=session_id,
        )


def main() -> None:
    """Execute raw-Kafka, catalog, and browser-download assertions once."""

    try:
        settings = Settings.from_environment()
        record = _consume_session_record(settings)
        session = validate_kafka_record(record, settings.session_id)
        detail = _wait_for_detail(settings)
        _validate_detail(detail, settings.session_id)
        _validate_list(settings, settings.session_id)
        _validate_download(settings, detail)
        _validate_mutation_is_unavailable(settings)
    except (ContractToDownloadError, ContractValidationError, KafkaError, OSError, requests.RequestException, ValueError) as error:
        raise SystemExit(f"Measurement-session contract-to-download test failed: {error}") from error

    print(
        "Measurement-session contract-to-download test passed for "
        f"{session.session_id}"
    )


def validate_kafka_record(record: Any, expected_session_id: str) -> MeasurementSession:
    """Require Kafka key, timestamp, and raw-Protobuf contract alignment."""

    message = MeasurementSession()
    try:
        message.ParseFromString(record.value)
    except DecodeError as error:
        raise ContractToDownloadError("MeasurementSession Kafka value is not raw Protobuf") from error
    validate_measurement_session(message)
    if message.session_id != expected_session_id:
        raise ContractToDownloadError("MeasurementSession Kafka record has an unexpected session ID")
    if record.key != expected_session_id.encode("utf-8"):
        raise ContractToDownloadError("MeasurementSession Kafka key does not match session ID")
    expected_timestamp = message.finalized_at.seconds * 1_000 + message.finalized_at.nanos // 1_000_000
    if record.timestamp != expected_timestamp:
        raise ContractToDownloadError("MeasurementSession Kafka timestamp does not match finalized_at")
    return message


def _consume_session_record(settings: Settings) -> Any:
    consumer: KafkaConsumer | None = None
    try:
        consumer = KafkaConsumer(
            settings.kafka_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
            client_id="measurement-session-e2e",
            group_id=f"measurement-session-e2e-{uuid4().hex}",
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            consumer_timeout_ms=settings.kafka_consume_timeout_seconds * 1_000,
            request_timeout_ms=30_000,
            api_version_auto_timeout_ms=10_000,
        )
        for record in consumer:
            if record.key == settings.session_id.encode("utf-8"):
                return record
    finally:
        if consumer is not None:
            consumer.close(autocommit=False)
    raise ContractToDownloadError("Kafka did not return the exported MeasurementSession record")


def _wait_for_detail(settings: Settings) -> dict[str, Any]:
    deadline = time.monotonic() + settings.http_timeout_seconds
    url = f"{settings.browser_url}/api/v1/measurement-sessions/{settings.session_id}"
    while time.monotonic() < deadline:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            payload = response.json()
            if not isinstance(payload, dict):
                raise ContractToDownloadError("Session detail response is malformed")
            return payload
        if response.status_code not in {404, 503}:
            raise ContractToDownloadError(f"Session detail returned HTTP {response.status_code}")
        time.sleep(0.5)
    raise ContractToDownloadError("Session catalog did not materialize before the HTTP timeout")


def _validate_detail(detail: dict[str, Any], session_id: str) -> None:
    if detail.get("session_id") != session_id:
        raise ContractToDownloadError("Catalog detail has an unexpected session ID")
    serialized = str(detail).lower()
    for forbidden in ("seaweed", "wama-s3", "object_key", "manifest_sha", "presigned"):
        if forbidden in serialized:
            raise ContractToDownloadError("Catalog detail exposed object-store implementation data")
    artifacts = detail.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) != 1:
        raise ContractToDownloadError("Catalog detail has an unexpected artifact list")


def _validate_list(settings: Settings, session_id: str) -> None:
    response = requests.get(
        f"{settings.browser_url}/api/v1/measurement-sessions?limit=100",
        timeout=5,
    )
    if response.status_code != 200:
        raise ContractToDownloadError(f"Session list returned HTTP {response.status_code}")
    payload = response.json()
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list) or not any(item.get("session_id") == session_id for item in items if isinstance(item, dict)):
        raise ContractToDownloadError("Session list does not contain the materialized session")


def _validate_download(settings: Settings, detail: dict[str, Any]) -> None:
    artifact = detail["artifacts"][0]
    if not isinstance(artifact, dict) or not isinstance(artifact.get("download_url"), str):
        raise ContractToDownloadError("Catalog detail artifact has no download path")
    download_name = artifact.get("download_name")
    if not isinstance(download_name, str):
        raise ContractToDownloadError("Catalog detail artifact has no download name")
    _validate_download_name(detail, download_name)
    download_url = artifact["download_url"]
    if not download_url.startswith("/v1/measurement-sessions/"):
        raise ContractToDownloadError("Catalog detail artifact has an unsafe download path")
    response = requests.get(f"{settings.browser_url}/api{download_url}", timeout=10, allow_redirects=False)
    expected_payload = Path("/app/fixture/waveform.csv").read_bytes()
    if response.status_code != 200:
        raise ContractToDownloadError(f"Artifact download returned HTTP {response.status_code}")
    if "location" in {name.lower() for name in response.headers}:
        raise ContractToDownloadError("Artifact download redirected away from the API")
    if response.headers.get("content-disposition") != f'attachment; filename="{download_name}"':
        raise ContractToDownloadError("Artifact download has an unexpected filename")
    if response.content != expected_payload or sha256(response.content).digest() != sha256(expected_payload).digest():
        raise ContractToDownloadError("Artifact download bytes do not match the exported fixture")
    validate_measurement_csv(response.content, detail)


def validate_measurement_csv(payload: bytes, detail: dict[str, Any]) -> None:
    """Require a complete ordered CSV series over the finalized interval."""

    measurement_count = detail.get("measurement_count")
    if not isinstance(measurement_count, int) or measurement_count <= 0:
        raise ContractToDownloadError("Catalog detail has an invalid measurement count")
    try:
        source = StringIO(payload.decode("utf-8"))
        validate_measurement_csv_stream(
            source,
            measurement_count,
            _parse_timestamp(detail.get("started_at"), "catalog start"),
            _parse_timestamp(detail.get("ended_at"), "catalog end"),
        )
    except (UnicodeError, MeasurementSeriesValidationError) as error:
        raise ContractToDownloadError(
            "Artifact download does not contain every declared measurement"
        ) from error


def _validate_download_name(detail: dict[str, Any], download_name: str) -> None:
    source_mrid = detail.get("source_mrid")
    if not isinstance(source_mrid, str):
        raise ContractToDownloadError("Catalog detail has no source MRID")
    source = re.sub(r"[^A-Za-z0-9]+", "-", source_mrid).strip("-").lower()[:80]
    started_on = _parse_timestamp(detail.get("started_at"), "catalog start").date().isoformat()
    if not source or not download_name.startswith(f"{source}_{started_on}_"):
        raise ContractToDownloadError("Artifact download name does not identify the source and date")


def _parse_timestamp(value: Any, location: str) -> datetime:
    if not isinstance(value, str):
        raise ContractToDownloadError(f"{location} timestamp is missing")
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ContractToDownloadError(f"{location} timestamp is malformed") from error
    if timestamp.tzinfo is None:
        raise ContractToDownloadError(f"{location} timestamp has no timezone")
    return timestamp.astimezone(timezone.utc)


def _validate_mutation_is_unavailable(settings: Settings) -> None:
    response = requests.post(f"{settings.browser_url}/api/v1/measurement-sessions", timeout=5)
    if response.status_code != 405:
        raise ContractToDownloadError("Catalog API unexpectedly accepts mutation requests")


def _required(values: dict[str, str], name: str, default: str | None = None) -> str:
    value = values.get(name, default or "").strip()
    if not value:
        raise ContractToDownloadError(f"{name} must not be empty")
    return value


def _url(values: dict[str, str], name: str, default: str) -> str:
    value = _required(values, name, default).rstrip("/")
    if not value.startswith(("http://", "https://")):
        raise ContractToDownloadError(f"{name} must be an HTTP URL")
    return value


def _positive_integer(values: dict[str, str], name: str, default: int) -> int:
    try:
        value = int(values.get(name, str(default)))
    except ValueError as error:
        raise ContractToDownloadError(f"{name} must be an integer") from error
    if value <= 0:
        raise ContractToDownloadError(f"{name} must be greater than zero")
    return value


def _positive_float(values: dict[str, str], name: str, default: float) -> float:
    try:
        value = float(values.get(name, str(default)))
    except ValueError as error:
        raise ContractToDownloadError(f"{name} must be a number") from error
    if value <= 0:
        raise ContractToDownloadError(f"{name} must be greater than zero")
    return value