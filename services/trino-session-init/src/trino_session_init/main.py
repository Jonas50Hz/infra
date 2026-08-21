"""Create the read-only-query Iceberg table through the internal writer."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os
from typing import Any, Protocol

import requests


class SessionInitError(RuntimeError):
    """Raised when the internal Iceberg table cannot be initialized safely."""


@dataclass(frozen=True)
class QueryResult:
    """Completed Trino statement result."""

    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]


@dataclass(frozen=True)
class Settings:
    """Environment-backed connection settings for the internal writer."""

    trino_url: str
    trino_user: str

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> "Settings":
        """Load the internal writer endpoint and require an explicit caller identity."""

        values = os.environ if environment is None else environment
        trino_url = _required(values, "TRINO_SESSION_INIT_URL", "http://trino-session-writer:8080")
        if not trino_url.startswith(("http://", "https://")):
            raise SessionInitError("TRINO_SESSION_INIT_URL must be an HTTP URL")
        return cls(
            trino_url=trino_url.rstrip("/"),
            trino_user=_required(values, "TRINO_SESSION_INIT_USER", "wama-session-init"),
        )


class StatementClient(Protocol):
    """Small boundary that keeps table initialization independently testable."""

    def execute(self, statement: str) -> QueryResult:
        """Execute one Trino statement or raise SessionInitError."""

    def close(self) -> None:
        """Release resources held by the client."""


class TrinoClient:
    """Follow Trino's paged statement API until one statement completes."""

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
        """Close the underlying HTTP session."""

        self._session.close()

    def execute(self, statement: str) -> QueryResult:
        """Submit a statement and collect all result pages."""

        payload = self._post(statement)
        columns: tuple[str, ...] = ()
        rows: list[tuple[Any, ...]] = []
        for _ in range(100):
            page_columns, page_rows, error, next_uri = _parse_page(payload)
            if page_columns:
                if columns and columns != page_columns:
                    raise SessionInitError("Trino statement changed result columns between pages")
                columns = page_columns
            rows.extend(page_rows)
            if error is not None:
                raise SessionInitError(f"Trino statement failed: {error}")
            if next_uri is None:
                return QueryResult(columns=columns, rows=tuple(rows))
            payload = self._get(next_uri)
        raise SessionInitError("Trino statement exceeded the supported result page limit")

    def _post(self, statement: str) -> Any:
        try:
            response = self._session.post(
                f"{self._base_url}/v1/statement",
                data=statement,
                headers=self._headers,
                timeout=15,
            )
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as error:
            raise SessionInitError(f"Trino statement submission failed: {error}") from error

    def _get(self, next_uri: str) -> Any:
        try:
            response = self._session.get(next_uri, headers=self._headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as error:
            raise SessionInitError(f"Trino statement result retrieval failed: {error}") from error


TABLE_COLUMNS = (
    "blob_id",
    "session_id",
    "timestamp_mccs",
    "mrid",
    "value_type",
    "double_value",
    "int_value",
    "uint_value",
    "bool_value",
    "string_value",
    "timestamp_value",
    "timestamp_field",
    "timestamp_gateway",
    "quality_valid",
    "quality_substituted",
    "quality_operator_blocked",
    "quality_overflow",
    "quality_old_data",
)

CREATE_SCHEMA = """
CREATE SCHEMA IF NOT EXISTS sessions.wama
WITH (location = 's3://wama-measurement-sessions/iceberg/wama')
""".strip()

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS sessions.wama.measurement_values (
    blob_id VARCHAR NOT NULL,
    session_id VARCHAR NOT NULL,
    timestamp_mccs TIMESTAMP(6) WITH TIME ZONE NOT NULL,
    mrid VARCHAR NOT NULL,
    value_type VARCHAR NOT NULL,
    double_value DOUBLE,
    int_value BIGINT,
    uint_value BIGINT,
    bool_value BOOLEAN,
    string_value VARCHAR,
    timestamp_value TIMESTAMP(6) WITH TIME ZONE,
    timestamp_field TIMESTAMP(6) WITH TIME ZONE,
    timestamp_gateway TIMESTAMP(6) WITH TIME ZONE,
    quality_valid BOOLEAN,
    quality_substituted BOOLEAN,
    quality_operator_blocked BOOLEAN,
    quality_overflow BOOLEAN,
    quality_old_data BOOLEAN
)
WITH (
    format = 'PARQUET',
    format_version = 2,
    location = 's3://wama-measurement-sessions/iceberg/wama/measurement_values'
)
""".strip()


def initialize(client: StatementClient) -> None:
    """Create and validate the fixed v2 table definition exactly once."""

    client.execute(CREATE_SCHEMA)
    client.execute(CREATE_TABLE)
    description = client.execute("DESCRIBE sessions.wama.measurement_values")
    columns = tuple(_column_name(row) for row in description.rows)
    if columns != TABLE_COLUMNS:
        raise SessionInitError("Iceberg measurement_values schema does not match session Parquet v2")


def main() -> None:
    """Initialize the fixed session query table before indexers begin consuming."""

    settings = Settings.from_environment()
    client = TrinoClient(settings.trino_url, settings.trino_user)
    try:
        initialize(client)
    finally:
        client.close()
    print("Measurement-session Iceberg table is ready.")


def _parse_page(payload: Any) -> tuple[tuple[str, ...], list[tuple[Any, ...]], str | None, str | None]:
    if not isinstance(payload, Mapping):
        raise SessionInitError("Trino statement response is not an object")
    error = payload.get("error")
    if error is not None:
        if isinstance(error, Mapping) and isinstance(error.get("message"), str):
            return (), [], error["message"], None
        return (), [], str(error), None
    columns = _columns(payload.get("columns"))
    rows = _rows(payload.get("data"))
    next_uri = payload.get("nextUri")
    if next_uri is not None and (
        not isinstance(next_uri, str) or not next_uri.startswith(("http://", "https://"))
    ):
        raise SessionInitError("Trino statement response has an invalid nextUri")
    return columns, rows, None, next_uri


def _columns(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise SessionInitError("Trino statement response has invalid columns")
    names: list[str] = []
    for column in value:
        if not isinstance(column, Mapping) or not isinstance(column.get("name"), str):
            raise SessionInitError("Trino statement response has an invalid column")
        names.append(column["name"])
    return tuple(names)


def _rows(value: Any) -> list[tuple[Any, ...]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise SessionInitError("Trino statement response has invalid data")
    rows: list[tuple[Any, ...]] = []
    for row in value:
        if not isinstance(row, list):
            raise SessionInitError("Trino statement response has an invalid row")
        rows.append(tuple(row))
    return rows


def _column_name(row: tuple[Any, ...]) -> str:
    if not row or not isinstance(row[0], str):
        raise SessionInitError("Iceberg table description has an invalid column row")
    return row[0]


def _required(values: Mapping[str, str], name: str, default: str) -> str:
    value = values.get(name, default).strip()
    if not value:
        raise SessionInitError(f"{name} must not be empty")
    return value


if __name__ == "__main__":
    main()