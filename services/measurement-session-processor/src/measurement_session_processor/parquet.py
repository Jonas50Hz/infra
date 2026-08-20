"""Stable long-form Parquet encoding for extracted Common Format values."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from measurement_session_processor.druid import MeasurementRow


class SessionArtifactError(RuntimeError):
    """Raised when a bounded request cannot produce a safe Parquet artifact."""


PARQUET_SCHEMA = pa.schema(
    [
        pa.field("timestamp_mccs", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("mrid", pa.string(), nullable=False),
        pa.field("value_type", pa.string(), nullable=False),
        pa.field("double_value", pa.float64()),
        pa.field("int_value", pa.int64()),
        pa.field("uint_value", pa.uint32()),
        pa.field("bool_value", pa.bool_()),
        pa.field("string_value", pa.string()),
        pa.field("timestamp_value", pa.timestamp("us", tz="UTC")),
        pa.field("timestamp_field", pa.timestamp("us", tz="UTC")),
        pa.field("timestamp_gateway", pa.timestamp("us", tz="UTC")),
        pa.field("quality_valid", pa.bool_()),
        pa.field("quality_substituted", pa.bool_()),
        pa.field("quality_operator_blocked", pa.bool_()),
        pa.field("quality_overflow", pa.bool_()),
        pa.field("quality_old_data", pa.bool_()),
    ]
)


@dataclass(frozen=True)
class ArtifactStats:
    """Integrity and coverage evidence calculated while writing one artifact."""

    measurement_count: int
    coverage: tuple[tuple[str, int], ...]
    sha256: bytes
    size_bytes: int


def write_session_parquet(
    destination: Path,
    rows: Iterable[MeasurementRow],
    requested_mrids: tuple[str, ...],
    max_rows: int,
    batch_rows: int,
    max_artifact_bytes: int,
) -> ArtifactStats:
    """Write sorted rows in batches and calculate immutable artifact evidence."""

    coverage = {mrid: 0 for mrid in requested_mrids}
    count = 0
    batch: list[dict[str, object]] = []
    try:
        writer = pq.ParquetWriter(
            destination,
            PARQUET_SCHEMA,
            compression="zstd",
            version="2.6",
            write_statistics=True,
        )
        try:
            for row in rows:
                if row.mrid not in coverage:
                    raise SessionArtifactError("Druid returned an MRID outside the requested session")
                count += 1
                if count > max_rows:
                    raise SessionArtifactError("measurement session exceeds configured maximum row count")
                coverage[row.mrid] += 1
                batch.append(row.as_parquet_row())
                if len(batch) == batch_rows:
                    _write_batch(writer, batch)
                    batch.clear()
            if batch:
                _write_batch(writer, batch)
        finally:
            writer.close()
    except (OSError, pa.ArrowException) as error:
        raise SessionArtifactError(f"Unable to write session Parquet artifact: {error}") from error

    digest, size_bytes = _file_digest(destination)
    if size_bytes > max_artifact_bytes:
        raise SessionArtifactError("measurement session exceeds configured maximum artifact size")
    return ArtifactStats(
        measurement_count=count,
        coverage=tuple((mrid, coverage[mrid]) for mrid in requested_mrids),
        sha256=digest,
        size_bytes=size_bytes,
    )


def _write_batch(writer: pq.ParquetWriter, rows: list[dict[str, object]]) -> None:
    writer.write_table(pa.Table.from_pylist(rows, schema=PARQUET_SCHEMA))


def _file_digest(path: Path) -> tuple[bytes, int]:
    hasher = sha256()
    size_bytes = 0
    with path.open("rb") as source:
        while chunk := source.read(64 * 1024):
            hasher.update(chunk)
            size_bytes += len(chunk)
    return hasher.digest(), size_bytes