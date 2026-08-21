"""Read-only Trino federation readiness evidence."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import math
from typing import Any

import requests

from infra_readiness.config import Settings


class TrinoReadinessError(RuntimeError):
    """Raised when read-only Trino federation is not ready."""


@dataclass(frozen=True)
class QueryResult:
    """Completed Trino statement result after following every response page."""

    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]
    error: str | None


class TrinoClient:
    """Small Trino HTTP client for deterministic readiness statements."""

    def __init__(
        self,
        base_url: str,
        user: str,
        session: requests.Session | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._headers = {"X-Trino-User": user}
        self._session = requests.Session() if session is None else session

    def close(self) -> None:
        """Close the HTTP session owned by this client."""

        self._session.close()

    def execute(self, statement: str) -> QueryResult:
        """Submit one statement and follow all result pages to completion."""

        payload = self._post_statement(statement)
        columns: tuple[str, ...] = ()
        rows: list[tuple[Any, ...]] = []
        for _ in range(100):
            page_columns, page_rows, error, next_uri = _parse_page(payload)
            if page_columns:
                if columns and columns != page_columns:
                    raise TrinoReadinessError("Trino statement changed result columns between pages")
                columns = page_columns
            rows.extend(page_rows)
            if error is not None:
                return QueryResult(columns=columns, rows=tuple(rows), error=error)
            if next_uri is None:
                return QueryResult(columns=columns, rows=tuple(rows), error=None)
            payload = self._get_page(next_uri)
        raise TrinoReadinessError("Trino statement exceeded the supported result page limit")

    def _post_statement(self, statement: str) -> Any:
        try:
            response = self._session.post(
                f"{self._base_url}/v1/statement",
                data=statement,
                headers=self._headers,
                timeout=10,
            )
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as error:
            raise TrinoReadinessError(f"Trino statement submission failed: {error}") from error

    def _get_page(self, next_uri: str) -> Any:
        try:
            response = self._session.get(next_uri, headers=self._headers, timeout=10)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as error:
            raise TrinoReadinessError(f"Trino statement result retrieval failed: {error}") from error


def check_trino(settings: Settings, require_live_measurement: bool = True) -> None:
    """Require read-only Druid, Blobmeta, and session Iceberg federation through Trino."""

    client = TrinoClient(settings.trino_url, settings.trino_user)
    try:
        validate_info(_get_info(client))
        catalogs = _successful_result(client.execute("SHOW CATALOGS"), "catalog discovery")
        validate_catalogs(
            catalogs,
            settings.trino_druid_catalog,
            settings.trino_blobmeta_catalog,
            settings.trino_session_catalog,
        )
        druid_schemas = _successful_result(
            client.execute(f"SHOW SCHEMAS FROM {settings.trino_druid_catalog}"),
            "Druid schema discovery",
        )
        validate_schema(druid_schemas, settings.trino_druid_schema, "Druid")
        blobmeta_tables = _successful_result(
            client.execute(
                "SHOW TABLES FROM "
                f"{settings.trino_blobmeta_catalog}.{settings.trino_blobmeta_schema}"
            ),
            "Blobmeta table discovery",
        )
        validate_blobmeta_tables(blobmeta_tables)
        session_tables = _successful_result(
            client.execute(
                "SHOW TABLES FROM "
                f"{settings.trino_session_catalog}.{settings.trino_session_schema}"
            ),
            "session table discovery",
        )
        validate_session_table(session_tables, settings.trino_session_table)
        if require_live_measurement:
            pmu_rows = _successful_result(
                client.execute(_pmu_query(settings)),
                "Druid PMU query",
            )
            validate_pmu_rows(
                pmu_rows,
                settings.druid_expected_mrid,
                settings.druid_expected_double_value,
                settings.druid_expected_double_value_tolerance,
            )
        write_attempt = client.execute(
            f"CREATE SCHEMA {settings.trino_blobmeta_catalog}.trino_read_only_probe"
        )
        validate_write_denial(write_attempt)
    finally:
        client.close()


def validate_info(payload: Any) -> None:
    """Require a started Trino coordinator response."""

    if (
        not isinstance(payload, Mapping)
        or payload.get("coordinator") is not True
        or payload.get("starting") is not False
        or not isinstance(payload.get("nodeVersion"), Mapping)
        or not payload["nodeVersion"].get("version")
    ):
        raise TrinoReadinessError("Trino info endpoint did not report a started coordinator")


def validate_catalogs(
    result: QueryResult,
    druid_catalog: str,
    blobmeta_catalog: str,
    session_catalog: str,
) -> None:
    """Require the configured Druid, Blobmeta, and session catalogs to be visible."""

    values = _first_column_values(result, "Trino catalog discovery")
    required = {druid_catalog, blobmeta_catalog, session_catalog}
    missing = required.difference(values)
    if missing:
        raise TrinoReadinessError(
            f"Trino catalog discovery is missing: {', '.join(sorted(missing))}"
        )


def validate_schema(result: QueryResult, schema: str, source: str) -> None:
    """Require one expected schema in a source catalog."""

    if schema not in _first_column_values(result, f"Trino {source} schema discovery"):
        raise TrinoReadinessError(f"Trino {source} catalog is missing schema {schema!r}")


def validate_blobmeta_tables(result: QueryResult) -> None:
    """Require immutable Blobmeta metadata and MRID coverage tables."""

    values = _first_column_values(result, "Trino Blobmeta table discovery")
    required = {"session_blobs", "session_blob_mrids"}
    missing = required.difference(values)
    if missing:
        raise TrinoReadinessError(
            f"Trino Blobmeta catalog is missing: {', '.join(sorted(missing))}"
        )


def validate_session_table(result: QueryResult, table: str) -> None:
    """Require the initialized public Iceberg session table."""

    if table not in _first_column_values(result, "Trino session table discovery"):
        raise TrinoReadinessError(f"Trino session catalog is missing table {table!r}")


def validate_pmu_rows(
    result: QueryResult,
    expected_mrid: str,
    expected_double_value: float,
    tolerance: float,
) -> None:
    """Require the configured valid PMU frequency through the Druid catalog."""

    if not result.rows:
        raise TrinoReadinessError("Trino Druid PMU query returned no rows")
    row = result.rows[0]
    if len(row) != 3 or row[0] != expected_mrid:
        raise TrinoReadinessError("Trino Druid PMU query returned an unexpected MRID")
    actual_value = row[1]
    if isinstance(actual_value, bool) or not isinstance(actual_value, (int, float)):
        raise TrinoReadinessError("Trino Druid PMU query has no numeric double_value")
    if not math.isclose(
        float(actual_value),
        expected_double_value,
        rel_tol=0.0,
        abs_tol=tolerance,
    ):
        raise TrinoReadinessError("Trino Druid PMU double_value is outside the expected range")
    if row[2] not in {True, "true", "TRUE"}:
        raise TrinoReadinessError("Trino Druid PMU row is not quality-valid")


def validate_write_denial(result: QueryResult) -> None:
    """Prove global Trino read-only access control rejects metadata writes."""

    if result.error is None:
        raise TrinoReadinessError("Trino read-only access control allowed a schema write")
    if "access denied" not in result.error.lower():
        raise TrinoReadinessError(
            f"Trino write attempt failed without an access-control denial: {result.error}"
        )


def _get_info(client: TrinoClient) -> Any:
    try:
        response = client._session.get(f"{client._base_url}/v1/info", timeout=10)
        response.raise_for_status()
        return response.json()
    except (requests.RequestException, ValueError) as error:
        raise TrinoReadinessError(f"Trino info request failed: {error}") from error


def _successful_result(result: QueryResult, label: str) -> QueryResult:
    if result.error is not None:
        raise TrinoReadinessError(f"Trino {label} failed: {result.error}")
    return result


def _parse_page(payload: Any) -> tuple[tuple[str, ...], list[tuple[Any, ...]], str | None, str | None]:
    if not isinstance(payload, Mapping):
        raise TrinoReadinessError("Trino statement response is not an object")
    error = payload.get("error")
    if error is not None:
        if isinstance(error, Mapping) and isinstance(error.get("message"), str):
            return (), [], error["message"], None
        return (), [], str(error), None
    columns = _columns(payload.get("columns"))
    rows = _rows(payload.get("data"))
    next_uri = payload.get("nextUri")
    if next_uri is not None and (not isinstance(next_uri, str) or not next_uri.startswith(("http://", "https://"))):
        raise TrinoReadinessError("Trino statement response has an invalid nextUri")
    return columns, rows, None, next_uri


def _columns(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise TrinoReadinessError("Trino statement response has invalid columns")
    names: list[str] = []
    for column in value:
        if not isinstance(column, Mapping) or not isinstance(column.get("name"), str):
            raise TrinoReadinessError("Trino statement response has an invalid column")
        names.append(column["name"])
    return tuple(names)


def _rows(value: Any) -> list[tuple[Any, ...]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise TrinoReadinessError("Trino statement response has invalid data")
    rows: list[tuple[Any, ...]] = []
    for row in value:
        if not isinstance(row, list):
            raise TrinoReadinessError("Trino statement response has an invalid row")
        rows.append(tuple(row))
    return rows


def _first_column_values(result: QueryResult, label: str) -> set[str]:
    values: set[str] = set()
    for row in result.rows:
        if not row or not isinstance(row[0], str):
            raise TrinoReadinessError(f"{label} returned an invalid row")
        values.add(row[0])
    return values


def _pmu_query(settings: Settings) -> str:
    escaped_mrid = settings.druid_expected_mrid.replace("'", "''")
    return (
        'SELECT mrid, double_value, quality_valid '
        f"FROM {settings.trino_druid_catalog}.{settings.trino_druid_schema}.live_measurements "
        f"WHERE mrid = '{escaped_mrid}' "
        'ORDER BY "__time" DESC LIMIT 1'
    )