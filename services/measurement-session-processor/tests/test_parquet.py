"""Tests for bounded Parquet artifact encoding."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import pyarrow.parquet as pq

from measurement_session_processor.druid import MeasurementRow
from measurement_session_processor.parquet import SessionArtifactError, write_session_parquet


class ParquetArtifactTests(unittest.TestCase):
    """Ensure the artifact remains typed, long-form, and coverage-aware."""

    def test_writes_empty_safe_schema_and_ordered_coverage(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "session.parquet"
            stats = write_session_parquet(
                path,
                [_row("urn:wama:poc:a"), _row("urn:wama:poc:a"), _row("urn:wama:poc:b")],
                ("urn:wama:poc:a", "urn:wama:poc:b"),
                max_rows=10,
                batch_rows=2,
                max_artifact_bytes=1_000_000,
            )
            table = pq.read_table(path)

        self.assertEqual(stats.measurement_count, 3)
        self.assertEqual(stats.coverage, (("urn:wama:poc:a", 2), ("urn:wama:poc:b", 1)))
        self.assertEqual(table.schema.field("value_type").type.__str__(), "string")
        self.assertEqual(table.num_rows, 3)

    def test_rejects_row_count_over_limit(self) -> None:
        with TemporaryDirectory() as directory:
            with self.assertRaisesRegex(SessionArtifactError, "maximum row count"):
                write_session_parquet(
                    Path(directory) / "session.parquet",
                    [_row("urn:wama:poc:a"), _row("urn:wama:poc:a")],
                    ("urn:wama:poc:a",),
                    max_rows=1,
                    batch_rows=10,
                    max_artifact_bytes=1_000_000,
                )


def _row(mrid: str) -> MeasurementRow:
    return MeasurementRow(
        timestamp_mccs=datetime(2026, 8, 19, 9, 0, tzinfo=timezone.utc),
        mrid=mrid,
        value_type="double",
        double_value=50.01,
    )