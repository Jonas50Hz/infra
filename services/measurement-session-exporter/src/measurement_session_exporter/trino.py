"""Small streaming client for the public read-only Trino coordinator."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any

import requests


class TrinoConnectionError(RuntimeError):
    """Raised when Trino cannot be reached while preparing an export."""


class TrinoStatementError(RuntimeError):
    """Raised when Trino cannot produce the fixed CSV query result."""


@dataclass(frozen=True)
class QueryPage:
    """One page of a completed or in-progress Trino result set."""

    columns: tuple[str, ...]
    rows: tuple[tuple[Any, ...], ...]


class TrinoClient:
    """Follow result pages without retaining a complete CSV export in memory."""

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
        """Release the HTTP connection pool."""

        self._session.close()

    def pages(self, statement: str) -> Iterator[QueryPage]:
        """Submit a statement and yield each Trino result page in order."""

        payload = self._post_statement(statement)
        for _ in range(10_000):
            page, next_uri = _parse_page(payload)
            yield page
            if next_uri is None:
                return
            payload = self._get_page(next_uri)
        raise TrinoStatementError("Trino statement exceeded the supported result page limit")

    def _post_statement(self, statement: str) -> Any:
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

    def _get_page(self, next_uri: str) -> Any:
        try:
            response = self._session.get(next_uri, headers=self._headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as error:
            raise TrinoConnectionError(f"Trino statement result retrieval failed: {error}") from error
        except ValueError as error:
            raise TrinoStatementError(f"Trino statement result is not JSON: {error}") from error


def _parse_page(payload: Any) -> tuple[QueryPage, str | None]:
    if not isinstance(payload, Mapping):
        raise TrinoStatementError("Trino statement response is not an object")
    error = payload.get("error")
    if error is not None:
        if isinstance(error, Mapping) and isinstance(error.get("message"), str):
            raise TrinoStatementError(error["message"])
        raise TrinoStatementError(str(error))
    next_uri = payload.get("nextUri")
    if next_uri is not None and (
        not isinstance(next_uri, str) or not next_uri.startswith(("http://", "https://"))
    ):
        raise TrinoStatementError("Trino statement response has an invalid nextUri")
    return QueryPage(columns=_columns(payload.get("columns")), rows=_rows(payload.get("data"))), next_uri


def _columns(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise TrinoStatementError("Trino statement columns are not an array")
    names: list[str] = []
    for column in value:
        if not isinstance(column, Mapping) or not isinstance(column.get("name"), str):
            raise TrinoStatementError("Trino statement has an invalid column")
        names.append(column["name"])
    return tuple(names)


def _rows(value: Any) -> tuple[tuple[Any, ...], ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise TrinoStatementError("Trino statement rows are not an array")
    rows: list[tuple[Any, ...]] = []
    for row in value:
        if not isinstance(row, list):
            raise TrinoStatementError("Trino statement has an invalid row")
        rows.append(tuple(row))
    return tuple(rows)