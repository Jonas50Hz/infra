"""HTTP behavior for MeasurementSession request submission."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import unittest

from fastapi.testclient import TestClient

from measurement_session_api.app import create_app
from measurement_session_api.config import Settings
from measurement_session_api.publisher import SessionPublishError


class _FakePublisher:
    def __init__(self, error: SessionPublishError | None = None) -> None:
        self.error = error
        self.requests = []
        self.closed = False

    def publish(self, request) -> None:
        if self.error is not None:
            raise self.error
        self.requests.append(request)

    def close(self) -> None:
        self.closed = True


class SessionApiTests(unittest.TestCase):
    """Validate requests before a Kafka acknowledgement produces HTTP success."""

    def setUp(self) -> None:
        self.settings = Settings(
            kafka_bootstrap_servers="kafka:9092",
            kafka_topic="MeasurementSession",
            max_interval_hours=24,
            max_mrids=32,
            publish_timeout_seconds=30,
            grafana_session_dashboard_url="http://grafana.example/d/wama-measurement-sessions",
        )
        self.now = datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc)

    def test_health_and_submit_normalize_selection(self) -> None:
        publisher = _FakePublisher()
        app = create_app(self.settings, publisher, clock=lambda: self.now)
        session_id = "d70ba792-70a6-42bd-8d15-7c75c4f5079d"

        with TestClient(app) as client:
            health = client.get("/healthz")
            response = client.post(
                "/v1/measurement-sessions",
                json={
                    "session_id": session_id,
                    "started_at": "2026-08-21T10:00:00Z",
                    "ended_at": "2026-08-21T10:30:00Z",
                    "mrids": ["urn:wama:poc:z", "urn:wama:poc:a", "urn:wama:poc:z"],
                    "capture_reason": "  operator review  ",
                },
            )

        self.assertEqual(health.json(), {"status": "ok"})
        self.assertEqual(response.status_code, 202)
        self.assertEqual(response.json()["session_id"], session_id)
        self.assertEqual(
            response.json()["blob_id"],
            f"sessions/{session_id}/measurements",
        )
        self.assertIn("var-blob_id=sessions%2F", response.json()["session_dashboard_url"])
        self.assertEqual(len(publisher.requests), 1)
        request = publisher.requests[0]
        self.assertEqual(list(request.mrids), ["urn:wama:poc:a", "urn:wama:poc:z"])
        self.assertEqual(
            [(entry.key, entry.value) for entry in request.metadata],
            [("capture_reason", "operator review"), ("request_origin", "grafana")],
        )
        self.assertEqual(request.requested_at.ToDatetime(tzinfo=timezone.utc), self.now)
        self.assertTrue(publisher.closed)

    def test_rejects_contract_violation_before_publication(self) -> None:
        publisher = _FakePublisher()
        app = create_app(self.settings, publisher, clock=lambda: self.now)

        with TestClient(app) as client:
            response = client.post(
                "/v1/measurement-sessions",
                json={
                    "session_id": "d70ba792-70a6-42bd-8d15-7c75c4f5079d",
                    "started_at": "2026-08-20T10:00:00Z",
                    "ended_at": "2026-08-21T11:00:00Z",
                    "mrids": ["urn:wama:poc:frequency"],
                },
            )

        self.assertEqual(response.status_code, 422)
        self.assertIn("measurement interval exceeds", response.json()["detail"])
        self.assertEqual(publisher.requests, [])

    def test_surfaces_kafka_unavailability(self) -> None:
        publisher = _FakePublisher(SessionPublishError("broker unavailable"))
        app = create_app(self.settings, publisher, clock=lambda: self.now)

        with TestClient(app) as client:
            response = client.post(
                "/v1/measurement-sessions",
                json={
                    "session_id": "d70ba792-70a6-42bd-8d15-7c75c4f5079d",
                    "started_at": "2026-08-21T10:00:00Z",
                    "ended_at": "2026-08-21T10:30:00Z",
                    "mrids": ["urn:wama:poc:frequency"],
                },
            )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["detail"], "broker unavailable")

    def test_serves_confirmation_page_without_shadowing_api(self) -> None:
        publisher = _FakePublisher()
        static_root = Path(__file__).resolve().parents[1] / "public"
        app = create_app(self.settings, publisher, clock=lambda: self.now, static_root=static_root)

        with TestClient(app) as client:
            page = client.get("/")
            health = client.get("/healthz")

        self.assertEqual(page.status_code, 200)
        self.assertIn("Measurement Session", page.text)
        self.assertEqual(health.json(), {"status": "ok"})


if __name__ == "__main__":
    unittest.main()