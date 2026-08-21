"""HTTP CSV download surface for selected immutable measurement sessions."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from io import StringIO
from typing import Any
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query, status
from fastapi.responses import StreamingResponse

from measurement_session_exporter.config import Settings
from measurement_session_exporter.trino import (
    QueryPage,
    TrinoClient,
    TrinoConnectionError,
    TrinoStatementError,
)


CSV_COLUMNS = ("time", "mrid", "value_type", "value")
MAX_MRIDS = 32
MAX_MRID_BYTES = 256
MAX_FILENAME_STEM_LENGTH = 240
VALUE_TYPES = frozenset({"double", "int", "uint"})


@dataclass(frozen=True)
class CsvExportSelection:
    """Validated Grafana selection for one immutable session artifact."""

    blob_id: str
    session_id: str
    mrids: tuple[str, ...]
    value_types: tuple[str, ...]
    from_milliseconds: int
    to_milliseconds: int

    @classmethod
    def from_parameters(
        cls,
        blob_id: str,
        mrids: Iterable[str],
        value_types: Iterable[str],
        from_milliseconds: int,
        to_milliseconds: int,
    ) -> "CsvExportSelection":
        """Validate and canonicalize values received through the dashboard link."""

        session_id = _session_id(blob_id)
        normalized_mrids = _mrids(mrids)
        normalized_value_types = _value_types(value_types)
        if from_milliseconds < 0 or to_milliseconds < 0:
            raise ValueError("time range must use non-negative epoch milliseconds")
        if from_milliseconds >= to_milliseconds:
            raise ValueError("from must be before to")
        return cls(
            blob_id=f"sessions/{session_id}/measurements",
            session_id=session_id,
            mrids=normalized_mrids,
            value_types=normalized_value_types,
            from_milliseconds=from_milliseconds,
            to_milliseconds=to_milliseconds,
        )


def create_app(
    settings: Settings,
    client_factory: Callable[[Settings], TrinoClient] = lambda values: TrinoClient(
        values.trino_url,
        values.trino_user,
    ),
) -> FastAPI:
    """Create the local CSV download application."""

    app = FastAPI(title="WAMA Measurement Session Exporter", docs_url=None, redoc_url=None)

    @app.get("/healthz")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/v1/measurement-sessions/export.csv")
    def export_csv(
        blob_id: str | None = Query(None),
        mrid: list[str] | None = Query(None),
        value_type: list[str] | None = Query(None),
        grafana_blob_id: str | None = Query(None, alias="var-blob_id"),
        grafana_mrid: list[str] | None = Query(None, alias="var-mrid"),
        grafana_value_type: list[str] | None = Query(None, alias="var-value_type"),
        from_milliseconds: int = Query(..., alias="from"),
        to_milliseconds: int = Query(..., alias="to"),
    ) -> StreamingResponse:
        try:
            selection = CsvExportSelection.from_parameters(
                _required_parameter("blob_id", blob_id, grafana_blob_id),
                _required_parameter("mrid", mrid, grafana_mrid),
                _required_parameter("value_type", value_type, grafana_value_type),
                from_milliseconds,
                to_milliseconds,
            )
        except ValueError as error:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(error)) from error

        client = client_factory(settings)
        try:
            pages = client.pages(_statement(selection))
            first_page = _first_result_page(pages)
            _validate_columns(first_page)
        except TrinoConnectionError as error:
            client.close()
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Trino is unavailable") from error
        except TrinoStatementError as error:
            client.close()
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="Trino export query failed") from error

        headers = {
            "Content-Disposition": f'attachment; filename="{_csv_filename(selection)}"'
        }
        return StreamingResponse(
            _csv_body(client, first_page, pages),
            media_type="text/csv",
            headers=headers,
        )

    return app


def _required_parameter(name: str, direct: Any, grafana: Any) -> Any:
    """Use one direct or Grafana URL parameter without accepting ambiguity."""

    if direct is None and grafana is None:
        raise ValueError(f"{name} is required")
    if direct is not None and grafana is not None and direct != grafana:
        raise ValueError(f"{name} has conflicting direct and Grafana values")
    return direct if direct is not None else grafana


def _session_id(blob_id: str) -> str:
    parts = blob_id.split("/")
    if len(parts) != 3 or parts[0] != "sessions" or parts[2] != "measurements":
        raise ValueError("blob_id must identify a completed measurement session")
    try:
        session_id = str(UUID(parts[1]))
    except ValueError as error:
        raise ValueError("blob_id must contain a canonical session UUID") from error
    if blob_id != f"sessions/{session_id}/measurements":
        raise ValueError("blob_id must use the canonical session path")
    return session_id


def _mrids(values: Iterable[str]) -> tuple[str, ...]:
    normalized = tuple(sorted({value.strip() for value in values if value.strip()}))
    if not 1 <= len(normalized) <= MAX_MRIDS:
        raise ValueError(f"mrid must contain between 1 and {MAX_MRIDS} selections")
    for mrid in normalized:
        if len(mrid.encode("utf-8")) > MAX_MRID_BYTES:
            raise ValueError(f"mrid values must not exceed {MAX_MRID_BYTES} UTF-8 bytes")
    return normalized


def _value_types(values: Iterable[str]) -> tuple[str, ...]:
    normalized = tuple(sorted({value.strip() for value in values if value.strip()}))
    if not normalized:
        raise ValueError("value_type must contain at least one selection")
    unexpected = set(normalized).difference(VALUE_TYPES)
    if unexpected:
        raise ValueError("value_type has an unsupported selection")
    return normalized


def _csv_filename(selection: CsvExportSelection) -> str:
    """Return a portable, bounded filename for the selected start time and MRIDs."""

    start_time = datetime.fromtimestamp(
        selection.from_milliseconds // 1_000,
        tz=timezone.utc,
    ).strftime("%Y-%m-%dT%H-%M-%SZ")
    prefix = f"measurement-session-{start_time}-"
    mrid_labels = "--".join(_filename_component(mrid) for mrid in selection.mrids)
    available_length = MAX_FILENAME_STEM_LENGTH - len(prefix)
    if len(mrid_labels) > available_length:
        digest = sha256("\0".join(selection.mrids).encode("utf-8")).hexdigest()[:12]
        marker = f"-selection-{len(selection.mrids)}-{digest}"
        mrid_labels = mrid_labels[: available_length - len(marker)].rstrip("-") + marker
    return f"{prefix}{mrid_labels}.csv"


def _filename_component(value: str) -> str:
    """Replace characters unsupported by common filesystems with one hyphen."""

    characters: list[str] = []
    separator_pending = False
    for character in value:
        if character.isascii() and (character.isalnum() or character in "-_."):
            characters.append(character)
            separator_pending = False
        elif not separator_pending:
            characters.append("-")
            separator_pending = True
    return "".join(characters).strip("-.") or "mrid"


def _statement(selection: CsvExportSelection) -> str:
    """Build the sole permitted read-only query with escaped literal values."""

    return (
        "SELECT to_iso8601(timestamp_mccs) AS time, mrid, value_type, "
        "COALESCE(double_value, CAST(int_value AS DOUBLE), CAST(uint_value AS DOUBLE)) AS value "
        "FROM sessions.wama.measurement_values "
        f"WHERE blob_id = {_literal(selection.blob_id)} "
        f"AND mrid IN ({_literals(selection.mrids)}) "
        f"AND value_type IN ({_literals(selection.value_types)}) "
        "AND timestamp_mccs >= "
        f"from_unixtime({_seconds(selection.from_milliseconds)}, 'UTC') "
        "AND timestamp_mccs <= "
        f"from_unixtime({_seconds(selection.to_milliseconds)}, 'UTC') "
        "ORDER BY timestamp_mccs ASC, mrid ASC, value_type ASC"
    )


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _literals(values: Iterable[str]) -> str:
    return ", ".join(_literal(value) for value in values)


def _seconds(milliseconds: int) -> str:
    seconds, remainder = divmod(milliseconds, 1_000)
    return f"{seconds}.{remainder:03d}"


def _first_result_page(pages: Iterator[QueryPage]) -> QueryPage:
    for page in pages:
        if page.columns:
            return page
    raise TrinoStatementError("Trino export query returned no result columns")


def _validate_columns(page: QueryPage) -> None:
    if page.columns != CSV_COLUMNS:
        raise TrinoStatementError("Trino export query returned unexpected columns")


def _csv_body(
    client: TrinoClient,
    first_page: QueryPage,
    pages: Iterator[QueryPage],
) -> Iterator[bytes]:
    try:
        yield _csv_line(CSV_COLUMNS)
        yield from _csv_rows(first_page)
        for page in pages:
            if page.columns:
                _validate_columns(page)
            yield from _csv_rows(page)
    finally:
        client.close()


def _csv_rows(page: QueryPage) -> Iterator[bytes]:
    for row in page.rows:
        if len(row) != len(CSV_COLUMNS):
            raise TrinoStatementError("Trino export query returned an invalid row")
        yield _csv_line(row)


def _csv_line(values: Iterable[Any]) -> bytes:
    buffer = StringIO(newline="")
    csv.writer(buffer, lineterminator="\n").writerow(values)
    return buffer.getvalue().encode("utf-8")