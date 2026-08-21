"""Internal Trino writer client for exact-file Iceberg registration."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

import requests

from measurement_session_query_indexer.artifact import VerifiedArtifact


class TrinoConnectionError(RuntimeError):
    """Raised when the internal writer endpoint is temporarily unavailable."""


class TrinoStatementError(RuntimeError):
    """Raised when a completed Trino statement violates the registration contract."""


@dataclass(frozen=True)
class QueryResult:
    """Completed paged Trino statement result."""

    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]


@dataclass(frozen=True)
class FileEntry:
    """One current Iceberg data-file entry."""

    path: str
    record_count: int
    byte_length: int


class StatementClient(Protocol):
    """Trino statement boundary used by the writer and unit tests."""

    def execute(self, statement: str) -> QueryResult:
        """Execute one statement or raise an appropriate Trino error."""


class TrinoClient:
    """HTTP client that follows Trino's result pages without polling loops."""

    def __init__(self, base_url: str, user: str, session: requests.Session | None = None) -> None:
        self._base_url = base_url.rstrip("/")
        self._headers = {"X-Trino-User": user}
        self._session = requests.Session() if session is None else session

    def close(self) -> None:
        """Release the underlying HTTP connection pool."""

        self._session.close()

    def execute(self, statement: str) -> QueryResult:
        """Submit a statement and return all result rows."""

        payload = self._post(statement)
        columns: tuple[str, ...] = ()
        rows: list[tuple[Any, ...]] = []
        for _ in range(100):
            page_columns, page_rows, error, next_uri = _parse_page(payload)
            if page_columns:
                if columns and columns != page_columns:
                    raise TrinoStatementError("Trino statement changed result columns between pages")
                columns = page_columns
            rows.extend(page_rows)
            if error is not None:
                raise TrinoStatementError(f"Trino statement failed: {error}")
            if next_uri is None:
                return QueryResult(columns=columns, rows=tuple(rows))
            payload = self._get(next_uri)
        raise TrinoStatementError("Trino statement exceeded the supported result page limit")

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
        except requests.RequestException as error:
            raise TrinoConnectionError(f"Trino statement submission failed: {error}") from error
        except ValueError as error:
            raise TrinoStatementError(f"Trino statement response is not JSON: {error}") from error

    def _get(self, next_uri: str) -> Any:
        try:
            response = self._session.get(next_uri, headers=self._headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as error:
            raise TrinoConnectionError(f"Trino statement result retrieval failed: {error}") from error
        except ValueError as error:
            raise TrinoStatementError(f"Trino statement result is not JSON: {error}") from error


class SessionWriter:
    """Register one verified canonical Parquet file or reconcile a replay."""

    def __init__(self, client: StatementClient) -> None:
        self._client = client

    def ensure_registered(self, artifact: VerifiedArtifact) -> FileEntry:
        """Add only the exact object URI, then require matching Iceberg evidence."""

        entry = self._find_file(artifact.object_uri)
        if entry is None:
            try:
                self._client.execute(_add_file_statement(artifact.object_uri))
            except TrinoStatementError:
                entry = self._find_file(artifact.object_uri)
                if entry is None:
                    raise
            if entry is None:
                entry = self._find_file(artifact.object_uri)
        if entry is None:
            raise TrinoStatementError("Iceberg registration returned without the canonical file")
        if entry.record_count != artifact.measurement_count or entry.byte_length != artifact.byte_length:
            raise TrinoStatementError("Iceberg file evidence does not match Blobmeta")
        return entry

    def _find_file(self, object_uri: str) -> FileEntry | None:
        result = self._client.execute(
            "SELECT file_path, record_count, file_size_in_bytes "
            "FROM sessions.wama.\"measurement_values$files\" "
            f"WHERE file_path = {_literal(object_uri)}"
        )
        if not result.rows:
            return None
        if len(result.rows) != 1:
            raise TrinoStatementError("Iceberg has duplicate current entries for one canonical object")
        path, record_count, byte_length = result.rows[0]
        if (
            not isinstance(path, str)
            or isinstance(record_count, bool)
            or not isinstance(record_count, int)
            or isinstance(byte_length, bool)
            or not isinstance(byte_length, int)
        ):
            raise TrinoStatementError("Iceberg file metadata has an invalid shape")
        return FileEntry(path=path, record_count=record_count, byte_length=byte_length)


def _add_file_statement(object_uri: str) -> str:
    return (
        "ALTER TABLE sessions.wama.measurement_values EXECUTE add_files("
        f"location => {_literal(object_uri)}, format => 'PARQUET')"
    )


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _parse_page(payload: Any) -> tuple[tuple[str, ...], list[tuple[Any, ...]], str | None, str | None]:
    if not isinstance(payload, Mapping):
        raise TrinoStatementError("Trino statement response is not an object")
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
        raise TrinoStatementError("Trino statement response has an invalid nextUri")
    return columns, rows, None, next_uri


def _columns(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise TrinoStatementError("Trino statement response has invalid columns")
    names: list[str] = []
    for column in value:
        if not isinstance(column, Mapping) or not isinstance(column.get("name"), str):
            raise TrinoStatementError("Trino statement response has an invalid column")
        names.append(column["name"])
    return tuple(names)


def _rows(value: Any) -> list[tuple[Any, ...]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise TrinoStatementError("Trino statement response has invalid data")
    rows: list[tuple[Any, ...]] = []
    for row in value:
        if not isinstance(row, list):
            raise TrinoStatementError("Trino statement response has an invalid row")
        rows.append(tuple(row))
    return rows