"""Tests for Trino federation readiness responses."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from infra_readiness.trino import (
    QueryResult,
    TrinoClient,
    TrinoReadinessError,
    validate_blobmeta_tables,
    validate_catalogs,
    validate_info,
    validate_pmu_rows,
    validate_session_table,
    validate_write_denial,
)


class TrinoReadinessTests(unittest.TestCase):
    """Exercise deterministic Trino metadata and query evidence."""

    def test_client_follows_result_pages(self) -> None:
        session = MagicMock()
        submitted = MagicMock()
        submitted.json.return_value = {"nextUri": "http://trino:8080/v1/statement/next"}
        completed = MagicMock()
        completed.json.return_value = {
            "columns": [{"name": "Catalog"}],
            "data": [["blobmeta"], ["druid"]],
        }
        session.post.return_value = submitted
        session.get.return_value = completed
        client = TrinoClient("http://trino:8080", "wama", session)

        result = client.execute("SHOW CATALOGS")

        self.assertEqual(result.columns, ("Catalog",))
        self.assertEqual(result.rows, (("blobmeta",), ("druid",)))
        session.post.assert_called_once()
        session.get.assert_called_once_with(
            "http://trino:8080/v1/statement/next",
            headers={"X-Trino-User": "wama"},
            timeout=10,
        )

    def test_accepts_expected_catalogs_and_blobmeta_tables(self) -> None:
        validate_catalogs(
            QueryResult(
                ("Catalog",),
                (("system",), ("druid",), ("blobmeta",), ("sessions",)),
                None,
            ),
            "druid",
            "blobmeta",
            "sessions",
        )
        validate_blobmeta_tables(
            QueryResult(
                ("Table",),
                (("session_blobs",), ("session_blob_mrids",)),
                None,
            )
        )

    def test_rejects_missing_blobmeta_table(self) -> None:
        with self.assertRaisesRegex(TrinoReadinessError, "session_blob_mrids"):
            validate_blobmeta_tables(
                QueryResult(("Table",), (("session_blobs",),), None)
            )

    def test_accepts_initialized_session_table(self) -> None:
        validate_session_table(
            QueryResult(("Table",), (("measurement_values",),), None),
            "measurement_values",
        )

    def test_rejects_missing_session_table(self) -> None:
        with self.assertRaisesRegex(TrinoReadinessError, "measurement_values"):
            validate_session_table(QueryResult(("Table",), (), None), "measurement_values")

    def test_accepts_expected_pmu_row(self) -> None:
        validate_pmu_rows(
            QueryResult(
                ("mrid", "double_value", "quality_valid"),
                (("urn:wama:poc:pmu:bay-01:frequency", 50.005, "true"),),
                None,
            ),
            "urn:wama:poc:pmu:bay-01:frequency",
            50.01,
            0.01,
        )

    def test_rejects_unexpected_write_failure(self) -> None:
        with self.assertRaisesRegex(TrinoReadinessError, "access-control denial"):
            validate_write_denial(QueryResult((), (), "Catalog is unavailable"))

    def test_accepts_access_denied_write_failure(self) -> None:
        validate_write_denial(
            QueryResult((), (), "Access Denied: Cannot create schema blobmeta.probe")
        )

    def test_rejects_non_coordinator_info(self) -> None:
        with self.assertRaisesRegex(TrinoReadinessError, "started coordinator"):
            validate_info(
                {
                    "coordinator": False,
                    "starting": False,
                    "nodeVersion": {"version": "483"},
                }
            )