"""Druid-specific readiness evidence for live measurements."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
import math
from typing import Any

import requests

from infra_readiness.config import Settings


class DruidReadinessError(RuntimeError):
    """Raised when Druid has not ingested a queryable live measurement."""


def check_druid(settings: Settings) -> None:
    """Require healthy Druid control-plane state and a PMU frequency query result."""

    session = requests.Session()
    try:
        router_health = _request_json(
            session,
            "Druid Router health",
            _url(settings.druid_router_url, "/status/health"),
        )
        supervisor_health = _request_json(
            session,
            "Druid supervisor health",
            _url(
                settings.druid_router_url,
                f"/druid/indexer/v1/supervisor/{settings.druid_supervisor_id}/health",
            ),
        )
        supervisor_status = _request_json(
            session,
            "Druid supervisor status",
            _url(
                settings.druid_router_url,
                f"/druid/indexer/v1/supervisor/{settings.druid_supervisor_id}/status",
            ),
        )
        rows = _post_json(
            session,
            "Druid live_measurements query",
            _url(settings.druid_router_url, "/druid/v2/sql"),
            {
                "query": _measurement_query(settings),
                "resultFormat": "object",
            },
        )
    finally:
        session.close()

    validate_router_health(router_health)
    validate_supervisor_health(supervisor_health)
    validate_supervisor_status(supervisor_status, settings.druid_supervisor_id)
    validate_measurement_rows(
        rows,
        settings.druid_expected_mrid,
        settings.druid_expected_double_value,
        settings.druid_expected_double_value_tolerance,
    )


def validate_router_health(payload: Any) -> None:
    """Accept both standard Druid health response shapes."""

    if payload is True:
        return
    if isinstance(payload, Mapping) and payload.get("healthy") is True:
        return
    raise DruidReadinessError("Druid Router health endpoint did not report healthy")


def validate_supervisor_health(payload: Any) -> None:
    """Require the live-measurement supervisor health endpoint to report healthy."""

    if payload is True:
        return
    if isinstance(payload, Mapping) and payload.get("healthy") is True:
        return
    raise DruidReadinessError("Druid supervisor health endpoint did not report healthy")


def validate_supervisor_status(payload: Any, supervisor_id: str) -> None:
    """Reject suspended, stopped, unhealthy, and parse-error Druid tasks."""

    if not isinstance(payload, Mapping):
        raise DruidReadinessError("Druid supervisor status did not return an object")
    status = payload.get("payload", payload)
    if not isinstance(status, Mapping):
        raise DruidReadinessError("Druid supervisor status payload did not return an object")
    actual_id = status.get("id", payload.get("id"))
    if actual_id not in {None, supervisor_id}:
        raise DruidReadinessError(f"Druid supervisor returned unexpected id {actual_id!r}")
    state = str(status.get("detailedState", status.get("state", ""))).upper()
    if state in {"", "SUSPENDED", "STOPPED", "UNHEALTHY"}:
        raise DruidReadinessError(f"Druid supervisor is not running: {state or '<missing>'}")
    errors = status.get("recentErrors", status.get("exceptions", []))
    if isinstance(errors, list) and errors:
        raise DruidReadinessError(f"Druid supervisor reported errors: {errors!r}")


def validate_measurement_rows(
    payload: Any,
    expected_mrid: str,
    expected_double_value: float,
    expected_double_value_tolerance: float = 0.0,
) -> None:
    """Require Druid SQL to return the configured raw-Protobuf PMU frequency row."""

    if not isinstance(payload, list) or not payload:
        raise DruidReadinessError("Druid live_measurements query returned no rows")
    row = payload[0]
    if not isinstance(row, Mapping):
        raise DruidReadinessError("Druid live_measurements query returned an invalid row")
    if row.get("mrid") != expected_mrid:
        raise DruidReadinessError(
            f"Druid live_measurements returned unexpected MRID {row.get('mrid')!r}"
        )
    actual_value = row.get("double_value")
    if isinstance(actual_value, bool) or not isinstance(actual_value, (int, float)):
        raise DruidReadinessError("Druid live_measurements row has no numeric double_value")
    if not math.isclose(
        float(actual_value),
        expected_double_value,
        rel_tol=0.0,
        abs_tol=expected_double_value_tolerance,
    ):
        raise DruidReadinessError(
            "Druid live_measurements double_value is outside the expected PMU range: "
            f"{actual_value!r}"
        )
    if row.get("quality_valid") not in {True, "true", "TRUE"}:
        raise DruidReadinessError("Druid live_measurements row is not quality-valid")
    druid_timestamp = _iso_timestamp_milliseconds(row.get("__time"), "__time")
    mccs_timestamp = _iso_timestamp_milliseconds(row.get("timestamp_mccs"), "timestamp_mccs")
    if druid_timestamp != mccs_timestamp:
        raise DruidReadinessError("Druid __time does not match timestamp_mccs")


def _measurement_query(settings: Settings) -> str:
    escaped_mrid = settings.druid_expected_mrid.replace("'", "''")
    return (
        'SELECT "__time", "mrid", "double_value", "quality_valid", "timestamp_mccs" '
        f'FROM "{settings.druid_datasource}" '
        f"WHERE \"mrid\" = '{escaped_mrid}' "
        'ORDER BY "__time" DESC LIMIT 1'
    )


def _iso_timestamp_milliseconds(value: Any, field_name: str) -> int:
    if not isinstance(value, str):
        raise DruidReadinessError(f"Druid live_measurements row has no {field_name}")
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise DruidReadinessError(
            f"Druid live_measurements {field_name} is not an ISO timestamp: {value!r}"
        ) from error
    if timestamp.tzinfo is None:
        raise DruidReadinessError(f"Druid live_measurements {field_name} has no timezone")
    return int(timestamp.timestamp() * 1_000)


def _request_json(session: requests.Session, label: str, url: str) -> Any:
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as error:
        raise DruidReadinessError(f"{label} request failed: {error}") from error
    try:
        return response.json()
    except ValueError as error:
        raise DruidReadinessError(f"{label} did not return JSON") from error


def _post_json(
    session: requests.Session,
    label: str,
    url: str,
    payload: Mapping[str, Any],
) -> Any:
    try:
        response = session.post(url, json=payload, timeout=10)
        response.raise_for_status()
    except requests.RequestException as error:
        raise DruidReadinessError(f"{label} request failed: {error}") from error
    try:
        return response.json()
    except ValueError as error:
        raise DruidReadinessError(f"{label} did not return JSON") from error


def _url(base_url: str, path: str) -> str:
    return f"{base_url.rstrip('/')}{path}"