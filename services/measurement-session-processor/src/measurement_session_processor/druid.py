"""Bounded Druid SQL extraction for MeasurementSession requests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any, Iterator

import requests

from measurement_session_common.generated.measurement_session_pb2 import MeasurementSessionRequest


class DruidQueryError(RuntimeError):
    """Raised when Druid cannot provide a well-formed bounded session result."""


@dataclass(frozen=True)
class MeasurementRow:
    """One Common Format value reconstructed from Druid's raw dimensions."""

    timestamp_mccs: datetime
    mrid: str
    value_type: str
    double_value: float | None = None
    int_value: int | None = None
    uint_value: int | None = None
    bool_value: bool | None = None
    string_value: str | None = None
    timestamp_value: datetime | None = None
    timestamp_field: datetime | None = None
    timestamp_gateway: datetime | None = None
    quality_valid: bool | None = None
    quality_substituted: bool | None = None
    quality_operator_blocked: bool | None = None
    quality_overflow: bool | None = None
    quality_old_data: bool | None = None

    @classmethod
    def from_druid(cls, row: Any) -> "MeasurementRow":
        """Validate one Druid object-line against the Common Format oneof rule."""

        if not isinstance(row, dict):
            raise DruidQueryError("Druid returned a non-object measurement row")
        mrid = row.get("mrid")
        if not isinstance(mrid, str) or not mrid:
            raise DruidQueryError("Druid measurement row has no MRID")
        value_fields = {
            "double": row.get("double_value"),
            "int": row.get("int_value"),
            "uint": row.get("uint_value"),
            "bool": row.get("bool_value"),
            "string": row.get("string_value"),
            "timestamp": row.get("timestamp_value"),
        }
        present = [(name, value) for name, value in value_fields.items() if value is not None]
        if len(present) != 1:
            raise DruidQueryError("Druid measurement row must contain exactly one scalar value")
        value_type, value = present[0]
        values: dict[str, Any] = {f"{value_type}_value": _value(value_type, value)}
        return cls(
            timestamp_mccs=_timestamp(row.get("timestamp_mccs"), "timestamp_mccs"),
            mrid=mrid,
            value_type=value_type,
            timestamp_field=_optional_timestamp(row.get("timestamp_field"), "timestamp_field"),
            timestamp_gateway=_optional_timestamp(
                row.get("timestamp_gateway"),
                "timestamp_gateway",
            ),
            quality_valid=_optional_boolean(row.get("quality_valid"), "quality_valid"),
            quality_substituted=_optional_boolean(
                row.get("quality_substituted"),
                "quality_substituted",
            ),
            quality_operator_blocked=_optional_boolean(
                row.get("quality_operator_blocked"),
                "quality_operator_blocked",
            ),
            quality_overflow=_optional_boolean(row.get("quality_overflow"), "quality_overflow"),
            quality_old_data=_optional_boolean(row.get("quality_old_data"), "quality_old_data"),
            **values,
        )

    def as_parquet_row(self) -> dict[str, Any]:
        """Translate the typed row to the stable long-form Parquet schema."""

        return {
            "timestamp_mccs": self.timestamp_mccs,
            "mrid": self.mrid,
            "value_type": self.value_type,
            "double_value": self.double_value,
            "int_value": self.int_value,
            "uint_value": self.uint_value,
            "bool_value": self.bool_value,
            "string_value": self.string_value,
            "timestamp_value": self.timestamp_value,
            "timestamp_field": self.timestamp_field,
            "timestamp_gateway": self.timestamp_gateway,
            "quality_valid": self.quality_valid,
            "quality_substituted": self.quality_substituted,
            "quality_operator_blocked": self.quality_operator_blocked,
            "quality_overflow": self.quality_overflow,
            "quality_old_data": self.quality_old_data,
        }


class DruidClient:
    """Read a time/MRID-bounded ordered result as Druid object lines."""

    def __init__(
        self,
        router_url: str,
        datasource: str,
        timeout_seconds: int,
        max_tied_rows: int = 5_000_000,
        session: requests.Session | None = None,
    ) -> None:
        self._router_url = router_url.rstrip("/")
        self._datasource = datasource
        self._timeout_seconds = timeout_seconds
        self._max_tied_rows = max_tied_rows
        self._session = requests.Session() if session is None else session

    def iter_rows(self, request: MeasurementSessionRequest) -> Iterator[MeasurementRow]:
        """Yield only the requested rows in deterministic timestamp/MRID order."""

        try:
            response = self._session.post(
                f"{self._router_url}/druid/v2/sql",
                json={
                    "query": query_for_request(request, self._datasource),
                    "resultFormat": "objectLines",
                },
                stream=True,
                timeout=self._timeout_seconds,
            )
            response.raise_for_status()
            try:
                rows = (
                    MeasurementRow.from_druid(json.loads(_line_text(raw_line)))
                    for raw_line in response.iter_lines(decode_unicode=True)
                    if raw_line
                )
                yield from _order_timestamp_ties(rows, self._max_tied_rows)
            finally:
                response.close()
        except (requests.RequestException, UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise DruidQueryError(f"Druid session query failed: {error}") from error


def query_for_request(request: MeasurementSessionRequest, datasource: str) -> str:
    """Build Druid SQL from validated values, escaping the MRID literals again."""

    escaped_mrids = ", ".join(f"'{mrid.replace("'", "''")}'" for mrid in request.mrids)
    start_milliseconds = _milliseconds(request.started_at.ToDatetime(tzinfo=timezone.utc))
    end_milliseconds = _milliseconds(request.ended_at.ToDatetime(tzinfo=timezone.utc))
    return (
        'SELECT "__time" AS "timestamp_mccs", "mrid", "double_value", "int_value", '
        '"uint_value", "bool_value", "string_value", "timestamp_value", '
        '"timestamp_field", "timestamp_gateway", "quality_valid", '
        '"quality_substituted", "quality_operator_blocked", "quality_overflow", '
        f'"quality_old_data" FROM "{datasource}" '
        f'WHERE "mrid" IN ({escaped_mrids}) '
        f'AND "__time" >= MILLIS_TO_TIMESTAMP({start_milliseconds}) '
        f'AND "__time" < MILLIS_TO_TIMESTAMP({end_milliseconds}) '
        'ORDER BY "__time" ASC'
    )


def _order_timestamp_ties(
    rows: Iterator[MeasurementRow],
    max_tied_rows: int,
) -> Iterator[MeasurementRow]:
    """Stabilize Druid's unspecified order among values with equal timestamps."""

    if max_tied_rows < 1:
        raise DruidQueryError("Druid tied-row limit must be positive")
    timestamp: datetime | None = None
    tied_rows: list[MeasurementRow] = []
    for row in rows:
        if timestamp is None:
            timestamp = row.timestamp_mccs
        elif row.timestamp_mccs < timestamp:
            raise DruidQueryError("Druid session rows are not ordered by timestamp")
        elif row.timestamp_mccs > timestamp:
            yield from sorted(tied_rows, key=_stable_row_key)
            timestamp = row.timestamp_mccs
            tied_rows = []
        tied_rows.append(row)
        if len(tied_rows) > max_tied_rows:
            raise DruidQueryError("Druid returned too many rows with one timestamp")
    yield from sorted(tied_rows, key=_stable_row_key)


def _line_text(value: str | bytes) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else value


def _stable_row_key(row: MeasurementRow) -> tuple[str, ...]:
    return (
        row.mrid,
        row.value_type,
        _stable_value(row.double_value),
        _stable_value(row.int_value),
        _stable_value(row.uint_value),
        _stable_value(row.bool_value),
        _stable_value(row.string_value),
        _stable_value(row.timestamp_value),
        _stable_value(row.timestamp_field),
        _stable_value(row.timestamp_gateway),
        _stable_value(row.quality_valid),
        _stable_value(row.quality_substituted),
        _stable_value(row.quality_operator_blocked),
        _stable_value(row.quality_overflow),
        _stable_value(row.quality_old_data),
    )


def _stable_value(value: object) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return repr(value)


def _milliseconds(value: datetime) -> int:
    return int(value.astimezone(timezone.utc).timestamp() * 1_000)


def _value(value_type: str, value: Any) -> Any:
    if value_type == "timestamp":
        return _timestamp(value, "timestamp_value")
    if value_type == "bool":
        parsed = _optional_boolean(value, "bool_value")
        if parsed is None:
            raise DruidQueryError("bool_value cannot be null")
        return parsed
    if value_type == "double":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise DruidQueryError("double_value is not numeric")
        return float(value)
    if value_type in {"int", "uint"}:
        if isinstance(value, bool) or not isinstance(value, int):
            raise DruidQueryError(f"{value_type}_value is not an integer")
        if value_type == "uint" and value < 0:
            raise DruidQueryError("uint_value is negative")
        return value
    if not isinstance(value, str):
        raise DruidQueryError("string_value is not text")
    return value


def _timestamp(value: Any, field_name: str) -> datetime:
    if not isinstance(value, str):
        raise DruidQueryError(f"Druid row has no {field_name}")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise DruidQueryError(f"Druid {field_name} is not an ISO timestamp") from error
    if parsed.tzinfo is None:
        raise DruidQueryError(f"Druid {field_name} has no timezone")
    return parsed.astimezone(timezone.utc)


def _optional_timestamp(value: Any, field_name: str) -> datetime | None:
    return None if value is None else _timestamp(value, field_name)


def _optional_boolean(value: Any, field_name: str) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.lower()
        if normalized == "true":
            return True
        if normalized == "false":
            return False
    raise DruidQueryError(f"Druid {field_name} is not boolean")