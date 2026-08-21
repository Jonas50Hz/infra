"""Tests for exact-file Iceberg registration through the internal writer."""

from __future__ import annotations

import unittest

from measurement_session_query_indexer.artifact import VerifiedArtifact
from measurement_session_query_indexer.trino import QueryResult, SessionWriter, TrinoStatementError


class SessionWriterTests(unittest.TestCase):
    """Require direct file registration and immutable file evidence."""

    def test_adds_only_the_exact_parquet_uri(self) -> None:
        artifact = _artifact()
        client = _Client(
            [
                QueryResult(columns=("file_path", "record_count", "file_size_in_bytes"), rows=()),
                QueryResult(columns=("metric_name", "metric_value"), rows=(("added_data_files", 1),)),
                QueryResult(
                    columns=("file_path", "record_count", "file_size_in_bytes"),
                    rows=((artifact.object_uri, 2, 1234),),
                ),
            ]
        )

        entry = SessionWriter(client).ensure_registered(artifact)

        self.assertEqual(entry.path, artifact.object_uri)
        self.assertIn("measurements.parquet", client.statements[1])
        self.assertNotIn("blobmeta.pb", client.statements[1])

    def test_rejects_mismatched_iceberg_file_evidence(self) -> None:
        artifact = _artifact()
        client = _Client(
            [
                QueryResult(
                    columns=("file_path", "record_count", "file_size_in_bytes"),
                    rows=((artifact.object_uri, 3, 1234),),
                )
            ]
        )

        with self.assertRaisesRegex(TrinoStatementError, "does not match"):
            SessionWriter(client).ensure_registered(artifact)


class _Client:
    def __init__(self, results: list[QueryResult]) -> None:
        self._results = iter(results)
        self.statements: list[str] = []

    def execute(self, statement: str) -> QueryResult:
        self.statements.append(statement)
        return next(self._results)


def _artifact() -> VerifiedArtifact:
    return VerifiedArtifact(
        blob_id="sessions/test/measurements",
        session_id="4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237",
        object_uri="s3://wama-measurement-sessions/sessions/test/measurements.parquet",
        byte_length=1234,
        sha256=b"x" * 32,
        measurement_count=2,
        coverage=(("urn:wama:poc:a", 2),),
    )