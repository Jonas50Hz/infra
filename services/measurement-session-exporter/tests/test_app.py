"""HTTP behavior for selected-session CSV export."""

from __future__ import annotations

from collections.abc import Iterator
import unittest

from fastapi.testclient import TestClient

from measurement_session_exporter.app import CsvExportSelection, create_app
from measurement_session_exporter.config import Settings
from measurement_session_exporter.trino import QueryPage, TrinoConnectionError


class _FakeClient:
    def __init__(self, pages: tuple[QueryPage, ...] = ()) -> None:
        self._pages = pages
        self.statements: list[str] = []
        self.closed = False

    def pages(self, statement: str) -> Iterator[QueryPage]:
        self.statements.append(statement)
        yield from self._pages

    def close(self) -> None:
        self.closed = True


class _UnavailableClient(_FakeClient):
    def pages(self, statement: str) -> Iterator[QueryPage]:
        del statement
        raise TrinoConnectionError("offline")
        yield


class CsvExporterTests(unittest.TestCase):
    """Keep the download route fixed-query, traceable, and bounded."""

    def setUp(self) -> None:
        self.settings = Settings("http://trino:8080", "measurement-session-exporter")
        self.blob_id = "sessions/d70ba792-70a6-42bd-8d15-7c75c4f5079d/measurements"

    def test_health_and_csv_export_stream_selected_chart_values(self) -> None:
        exporter = _FakeClient(
            (
                QueryPage((), ()),
                QueryPage(
                    ("time", "mrid", "value_type", "value"),
                    (
                        ("2026-08-21T10:00:00Z", "urn:wama:poc:frequency", "double", 50.0),
                        ("2026-08-21T10:01:00Z", "urn:wama:poc:frequency", "double", 50.1),
                    ),
                ),
            )
        )
        app = create_app(self.settings, client_factory=lambda _: exporter)

        with TestClient(app) as client:
            health = client.get("/healthz")
            response = client.get(
                "/v1/measurement-sessions/export.csv",
                params=[
                    ("var-blob_id", self.blob_id),
                    ("var-mrid", "urn:wama:poc:frequency"),
                    ("var-value_type", "double"),
                    ("from", "1787306400000"),
                    ("to", "1787308200000"),
                ],
            )

        self.assertEqual(health.json(), {"status": "ok"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "text/csv; charset=utf-8")
        self.assertEqual(
            response.headers["content-disposition"],
            (
                "attachment; filename=\"measurement-session-"
                "2026-08-21T10-00-00Z-"
                "urn-wama-poc-frequency.csv\""
            ),
        )
        self.assertEqual(
            response.text,
            "time,mrid,value_type,value\n"
            "2026-08-21T10:00:00Z,urn:wama:poc:frequency,double,50.0\n"
            "2026-08-21T10:01:00Z,urn:wama:poc:frequency,double,50.1\n",
        )
        self.assertIn("FROM sessions.wama.measurement_values", exporter.statements[0])
        self.assertIn("blob_id = 'sessions/d70ba792-70a6-42bd-8d15-7c75c4f5079d/measurements'", exporter.statements[0])
        self.assertIn("from_unixtime(1787306400.000, 'UTC')", exporter.statements[0])
        self.assertTrue(exporter.closed)

    def test_csv_filename_includes_multiple_selected_mrids(self) -> None:
        exporter = _FakeClient((QueryPage(("time", "mrid", "value_type", "value"), ()),))
        app = create_app(self.settings, client_factory=lambda _: exporter)

        with TestClient(app) as client:
            response = client.get(
                "/v1/measurement-sessions/export.csv",
                params=[
                    ("var-blob_id", self.blob_id),
                    ("var-mrid", "urn:wama:poc:frequency"),
                    ("var-mrid", "urn:wama:poc:current-l1"),
                    ("var-value_type", "double"),
                    ("from", "1787306400000"),
                    ("to", "1787308200000"),
                ],
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.headers["content-disposition"],
            (
                "attachment; filename=\"measurement-session-"
                "2026-08-21T10-00-00Z-"
                "urn-wama-poc-current-l1--urn-wama-poc-frequency.csv\""
            ),
        )

    def test_rejects_noncanonical_or_unbounded_dashboard_selection(self) -> None:
        exporter = _FakeClient()
        app = create_app(self.settings, client_factory=lambda _: exporter)

        with TestClient(app) as client:
            malformed = client.get(
                "/v1/measurement-sessions/export.csv",
                params={
                    "blob_id": "sessions/not-a-uuid/measurements",
                    "mrid": "urn:wama:poc:frequency",
                    "value_type": "double",
                    "from": "1787306400000",
                    "to": "1787308200000",
                },
            )
            unsupported_type = client.get(
                "/v1/measurement-sessions/export.csv",
                params={
                    "blob_id": self.blob_id,
                    "mrid": "urn:wama:poc:frequency",
                    "value_type": "string",
                    "from": "1787306400000",
                    "to": "1787308200000",
                },
            )

        self.assertEqual(malformed.status_code, 422)
        self.assertIn("canonical session UUID", malformed.json()["detail"])
        self.assertEqual(unsupported_type.status_code, 422)
        self.assertIn("unsupported", unsupported_type.json()["detail"])
        self.assertEqual(exporter.statements, [])

    def test_escapes_selected_mrids_as_sql_literals(self) -> None:
        selection = CsvExportSelection.from_parameters(
            self.blob_id,
            ("urn:wama:poc:frequency' OR 1=1 --",),
            ("double",),
            1787306400000,
            1787308200000,
        )

        statement = __import__("measurement_session_exporter.app", fromlist=["_statement"])._statement(selection)

        self.assertIn("mrid IN ('urn:wama:poc:frequency'' OR 1=1 --')", statement)

    def test_surfaces_trino_unavailability_before_starting_the_download(self) -> None:
        app = create_app(self.settings, client_factory=lambda _: _UnavailableClient())

        with TestClient(app) as client:
            response = client.get(
                "/v1/measurement-sessions/export.csv",
                params={
                    "blob_id": self.blob_id,
                    "mrid": "urn:wama:poc:frequency",
                    "value_type": "double",
                    "from": "1787306400000",
                    "to": "1787308200000",
                },
            )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["detail"], "Trino is unavailable")


if __name__ == "__main__":
    unittest.main()