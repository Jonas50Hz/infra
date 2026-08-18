"""Druid Kafka supervisor creation and health validation."""

from __future__ import annotations

from collections.abc import Mapping
import json
from pathlib import Path
from typing import Any

import requests


class SupervisorError(RuntimeError):
    """Raised when the live-measurement supervisor is absent or unhealthy."""


def load_supervisor_spec(path: Path, expected_supervisor_id: str) -> dict[str, Any]:
    """Read and validate the immutable live-measurement supervisor specification."""

    try:
        contents = path.read_text(encoding="utf-8")
    except OSError as error:
        raise SupervisorError(f"Druid supervisor specification is unavailable: {path}") from error
    try:
        specification = json.loads(contents)
    except json.JSONDecodeError as error:
        raise SupervisorError(f"Druid supervisor specification is invalid JSON: {path}") from error
    if not isinstance(specification, dict):
        raise SupervisorError("Druid supervisor specification must be a JSON object")
    validate_supervisor_spec(specification, expected_supervisor_id)
    return specification


def validate_supervisor_spec(specification: Mapping[str, Any], expected_supervisor_id: str) -> None:
    """Reject a supervisor that could aggregate or decode a different data contract."""

    if specification.get("type") != "kafka":
        raise SupervisorError("Druid supervisor type must be kafka")
    supervisor = _mapping(specification, "spec", "Druid supervisor specification has no spec")
    data_schema = _mapping(supervisor, "dataSchema", "Druid supervisor has no dataSchema")
    if data_schema.get("dataSource") != expected_supervisor_id:
        raise SupervisorError(
            f"Druid datasource must be {expected_supervisor_id!r}; "
            f"found {data_schema.get('dataSource')!r}"
        )
    granularity = _mapping(data_schema, "granularitySpec", "Druid supervisor has no granularitySpec")
    if granularity.get("queryGranularity") != "none" or granularity.get("rollup") is not False:
        raise SupervisorError("Druid live_measurements must disable query granularity and rollup")

    io_config = _mapping(supervisor, "ioConfig", "Druid supervisor has no ioConfig")
    if io_config.get("type") != "kafka" or io_config.get("topic") != "LiveMeasurement":
        raise SupervisorError("Druid supervisor must consume the LiveMeasurement Kafka topic")
    input_format = _mapping(io_config, "inputFormat", "Druid supervisor has no Kafka input format")
    if input_format.get("type") != "kafka":
        raise SupervisorError("Druid supervisor must use the Kafka input format")
    value_format = _mapping(input_format, "valueFormat", "Druid Kafka input format has no value format")
    if value_format.get("type") != "protobuf":
        raise SupervisorError("Druid supervisor must use the Protobuf input format")
    decoder = _mapping(value_format, "protoBytesDecoder", "Druid Protobuf input format has no decoder")
    if decoder.get("type") != "file":
        raise SupervisorError("Druid Protobuf decoder must use the built descriptor file")
    if decoder.get("descriptor") != "file:///opt/wama/rtd_schema.desc":
        raise SupervisorError("Druid Protobuf decoder must use the canonical built descriptor")
    if decoder.get("protoMessageType") != "rtd_schema.v1.MCCSMeasurementValue":
        raise SupervisorError("Druid Protobuf decoder must use MCCSMeasurementValue")


def submit_supervisor(
    session: requests.Session,
    router_url: str,
    specification: Mapping[str, Any],
) -> None:
    """Create or update the supervisor without restarting an unchanged task."""

    try:
        response = session.post(
            _url(router_url, "/druid/indexer/v1/supervisor"),
            params={"skipRestartIfUnmodified": "true"},
            json=specification,
            timeout=10,
        )
        response.raise_for_status()
    except requests.RequestException as error:
        raise SupervisorError(f"Druid supervisor submission failed: {error}") from error


def check_supervisor(session: requests.Session, router_url: str, supervisor_id: str) -> None:
    """Require a healthy active supervisor with no saved ingestion errors."""

    health = _request_json(
        session,
        "Druid supervisor health",
        _url(router_url, f"/druid/indexer/v1/supervisor/{supervisor_id}/health"),
    )
    validate_supervisor_health(health)
    status = _request_json(
        session,
        "Druid supervisor status",
        _url(router_url, f"/druid/indexer/v1/supervisor/{supervisor_id}/status"),
    )
    validate_supervisor_status(status, supervisor_id)


def validate_supervisor_health(payload: Any) -> None:
    """Accept both documented Druid health response shapes."""

    if payload is True:
        return
    if isinstance(payload, Mapping) and payload.get("healthy") is True:
        return
    raise SupervisorError("Druid supervisor health endpoint did not report healthy")


def validate_supervisor_status(payload: Any, supervisor_id: str) -> None:
    """Reject stopped, suspended, and parse-error supervisor states."""

    if not isinstance(payload, Mapping):
        raise SupervisorError("Druid supervisor status did not return an object")
    status = payload.get("payload", payload)
    if not isinstance(status, Mapping):
        raise SupervisorError("Druid supervisor status payload did not return an object")
    actual_id = status.get("id", payload.get("id"))
    if actual_id not in {None, supervisor_id}:
        raise SupervisorError(f"Druid supervisor status returned unexpected id {actual_id!r}")
    state = str(status.get("detailedState", status.get("state", ""))).upper()
    if state in {"", "SUSPENDED", "STOPPED", "UNHEALTHY"}:
        raise SupervisorError(f"Druid supervisor is not running: {state or '<missing>'}")
    errors = status.get("recentErrors", status.get("exceptions", []))
    if isinstance(errors, list) and errors:
        raise SupervisorError(f"Druid supervisor reported errors: {errors!r}")


def _mapping(value: Mapping[str, Any], key: str, error_message: str) -> Mapping[str, Any]:
    nested = value.get(key)
    if not isinstance(nested, Mapping):
        raise SupervisorError(error_message)
    return nested


def _request_json(session: requests.Session, label: str, url: str) -> Any:
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as error:
        raise SupervisorError(f"{label} request failed: {error}") from error
    try:
        return response.json()
    except ValueError as error:
        raise SupervisorError(f"{label} did not return JSON") from error


def _url(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}{path}"