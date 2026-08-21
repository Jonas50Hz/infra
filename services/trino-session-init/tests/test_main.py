"""Tests for the immutable measurement-session Iceberg table bootstrap."""

from __future__ import annotations

import unittest

from trino_session_init.main import (
    CREATE_SCHEMA,
    CREATE_TABLE,
    TABLE_COLUMNS,
    QueryResult,
    SessionInitError,
    Settings,
    initialize,
)


class SessionInitTests(unittest.TestCase):
    """Ensure initialization remains idempotent and schema-specific."""

    def test_initializes_the_expected_v2_table(self) -> None:
        client = _Client(TABLE_COLUMNS)

        initialize(client)

        self.assertEqual(
            client.statements,
            [CREATE_SCHEMA, CREATE_TABLE, "DESCRIBE sessions.wama.measurement_values"],
        )
        self.assertIn("blob_id VARCHAR NOT NULL", CREATE_TABLE)
        self.assertIn("timestamp_mccs TIMESTAMP(6) WITH TIME ZONE NOT NULL", CREATE_TABLE)
        self.assertIn("format_version = 2", CREATE_TABLE)

    def test_rejects_an_existing_table_with_a_different_schema(self) -> None:
        with self.assertRaisesRegex(SessionInitError, "does not match"):
            initialize(_Client(TABLE_COLUMNS[:-1]))

    def test_rejects_a_non_http_writer_url(self) -> None:
        with self.assertRaisesRegex(SessionInitError, "HTTP URL"):
            Settings.from_environment({"TRINO_SESSION_INIT_URL": "postgres://postgres"})


class _Client:
    def __init__(self, columns: tuple[str, ...]) -> None:
        self._columns = columns
        self.statements: list[str] = []

    def execute(self, statement: str) -> QueryResult:
        self.statements.append(statement)
        if statement == "DESCRIBE sessions.wama.measurement_values":
            return QueryResult(
                columns=("Column", "Type", "Extra", "Comment"),
                rows=tuple((column, "", "", "") for column in self._columns),
            )
        return QueryResult(columns=(), rows=())

    def close(self) -> None:
        return None