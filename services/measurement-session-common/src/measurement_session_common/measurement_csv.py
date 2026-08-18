"""Validate CSV waveform artifacts against immutable finalized-session bounds."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from typing import TextIO


MEASUREMENT_CSV_ARTIFACT_ID = "waveform"
MEASUREMENT_CSV_CONTENT_TYPE = "text/csv"


class MeasurementSeriesValidationError(ValueError):
    """Raised when a waveform CSV does not contain one complete session series."""


def is_measurement_csv_artifact(artifact_id: str, content_type: str) -> bool:
    """Identify the PoC artifact that carries every scalar measurement sample."""

    return (
        artifact_id == MEASUREMENT_CSV_ARTIFACT_ID
        and content_type.split(";", maxsplit=1)[0].strip().lower()
        == MEASUREMENT_CSV_CONTENT_TYPE
    )


def validate_measurement_csv(
    source: TextIO,
    measurement_count: int,
    started_at: datetime,
    ended_at: datetime,
) -> None:
    """Require a complete, ordered CSV series over the finalized interval."""

    if measurement_count <= 0:
        raise MeasurementSeriesValidationError("measurement_count must be greater than zero")
    if started_at.tzinfo is None or ended_at.tzinfo is None:
        raise MeasurementSeriesValidationError("session bounds must include timezones")

    reader = csv.DictReader(source)
    headers = reader.fieldnames
    if not headers or any(not header for header in headers):
        raise MeasurementSeriesValidationError("measurement CSV must have named columns")
    if len(headers) != len(set(headers)):
        raise MeasurementSeriesValidationError("measurement CSV column names must be unique")
    if "timestamp" not in headers:
        raise MeasurementSeriesValidationError("measurement CSV must include a timestamp column")
    value_columns = tuple(column for column in headers if column != "timestamp")
    if not value_columns:
        raise MeasurementSeriesValidationError("measurement CSV must include value columns")

    expected_start = started_at.astimezone(timezone.utc)
    expected_end = ended_at.astimezone(timezone.utc)
    previous: datetime | None = None
    first: datetime | None = None
    last: datetime | None = None
    count = 0
    for row in reader:
        if None in row or any(not (row.get(column) or "").strip() for column in headers):
            raise MeasurementSeriesValidationError("measurement CSV has incomplete values")
        timestamp = _parse_timestamp(row["timestamp"])
        if previous is not None and timestamp <= previous:
            raise MeasurementSeriesValidationError("measurement CSV timestamps must be strictly ordered")
        first = first or timestamp
        previous = timestamp
        last = timestamp
        count += 1

    if count != measurement_count:
        raise MeasurementSeriesValidationError(
            "measurement CSV row count does not match measurement_count"
        )
    if first != expected_start or last != expected_end:
        raise MeasurementSeriesValidationError(
            "measurement CSV timestamps do not match the finalized session bounds"
        )


def _parse_timestamp(value: str) -> datetime:
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise MeasurementSeriesValidationError("measurement CSV has a malformed timestamp") from error
    if timestamp.tzinfo is None:
        raise MeasurementSeriesValidationError("measurement CSV timestamps must include timezones")
    return timestamp.astimezone(timezone.utc)