"""Tests for anonymous, read-only API behavior and credential-free responses."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO
import unittest

from fastapi.testclient import TestClient

from measurement_session_common.manifest import ArtifactDescriptor, SessionManifest
from measurement_session_api.app import create_app
from measurement_session_api.catalog import CatalogSession, ManifestReference
from measurement_session_api.storage import (
    ArtifactStream,
    IncompleteMeasurementDataError,
    ObjectStoreError,
)


class _Catalog:
    def __init__(self, sessions: CatalogSession | tuple[CatalogSession, ...]) -> None:
        self.sessions = sessions if isinstance(sessions, tuple) else (sessions,)
        self.initialized = False

    def initialize(self) -> None:
        self.initialized = True

    def insert(self, session: CatalogSession) -> None:
        self.sessions = (session,)

    def get(self, session_id: str) -> CatalogSession | None:
        return next((session for session in self.sessions if session_id == session.session_id), None)

    def list_sessions(self, limit: int, after):
        del after
        return self.sessions[:limit]


class _Worker:
    def __init__(self, ready: bool = True) -> None:
        self.failure: str | None = None
        self.ready = ready

    def start(self) -> None:
        return None

    def stop(self) -> None:
        return None


class _Store:
    def __init__(self, fail: bool = False, incomplete_session_ids: frozenset[str] = frozenset()) -> None:
        self.fail = fail
        self.incomplete_session_ids = incomplete_session_ids
        self._descriptor = ArtifactDescriptor(
            artifact_id="waveform",
            object_key="sessions/4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237/artifacts/waveform.csv",
            content_type="text/csv",
            size_bytes=7,
            sha256_hex=sha256(b"sample\n").hexdigest(),
        )

    def verified_manifest(self, session: CatalogSession) -> SessionManifest:
        if self.fail:
            raise ObjectStoreError("tampered")
        if session.session_id in self.incomplete_session_ids:
            raise IncompleteMeasurementDataError("waveform has only boundary rows")
        return SessionManifest(
            session_id="4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237",
            artifacts=(self._descriptor,),
        )

    def artifact(self, session: CatalogSession, artifact_id: str):
        del session
        if self.fail:
            raise ObjectStoreError("tampered")
        if artifact_id != self._descriptor.artifact_id:
            from measurement_session_api.storage import ArtifactNotFoundError

            raise ArtifactNotFoundError("missing")
        return self._descriptor, ArtifactStream(BytesIO(b"sample\n"), "text/csv", 7)


class ReadOnlyApiTests(unittest.TestCase):
    """Verify the browser-facing API does not reveal storage implementation."""

    def test_detail_and_download_are_read_only_and_hide_s3_data(self) -> None:
        app = create_app(_Catalog(_session()), _Store(), _Worker())

        with TestClient(app) as client:
            detail = client.get("/v1/measurement-sessions/4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237")
            download = client.get("/v1/measurement-sessions/4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237/artifacts/waveform")
            mutation = client.post("/v1/measurement-sessions")

        self.assertEqual(detail.status_code, 200)
        self.assertNotIn("wama-s3", detail.text)
        self.assertNotIn("seaweed", detail.text)
        self.assertEqual(
            detail.json()["artifacts"][0]["download_name"],
            "urn-wama-poc-pmu-bay-01_2026-08-18_waveform.csv",
        )
        self.assertEqual(download.status_code, 200)
        self.assertEqual(download.content, b"sample\n")
        self.assertEqual(
            download.headers["content-disposition"],
            'attachment; filename="urn-wama-poc-pmu-bay-01_2026-08-18_waveform.csv"',
        )
        self.assertNotIn("location", {key.lower() for key in download.headers})
        self.assertEqual(mutation.status_code, 405)

    def test_manifest_failure_refuses_detail(self) -> None:
        app = create_app(_Catalog(_session()), _Store(fail=True), _Worker())

        with TestClient(app) as client:
            response = client.get("/v1/measurement-sessions/4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237")

        self.assertEqual(response.status_code, 503)

    def test_health_waits_for_catalog_worker_connection(self) -> None:
        app = create_app(_Catalog(_session()), _Store(), _Worker(ready=False))

        with TestClient(app) as client:
            response = client.get("/healthz")

        self.assertEqual(response.status_code, 503)

    def test_list_hides_sessions_with_incomplete_waveforms(self) -> None:
        complete = _session()
        incomplete = replace(complete, session_id="bb11a4c6-1ae4-4f51-b1b7-d7762a7c4237")
        app = create_app(
            _Catalog((incomplete, complete)),
            _Store(incomplete_session_ids=frozenset({incomplete.session_id})),
            _Worker(),
        )

        with TestClient(app) as client:
            response = client.get("/v1/measurement-sessions")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            [item["session_id"] for item in response.json()["items"]],
            [complete.session_id],
        )


def _session() -> CatalogSession:
    return CatalogSession(
        session_id="4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237",
        source_mrid="urn:wama:poc:pmu:bay-01",
        started_at=datetime(2026, 8, 18, 9, 0, tzinfo=timezone.utc),
        ended_at=datetime(2026, 8, 18, 9, 1, tzinfo=timezone.utc),
        finalized_at=datetime(2026, 8, 18, 9, 2, tzinfo=timezone.utc),
        metadata=(("asset", "bay-01"),),
        measurement_count=12,
        artifact_count=1,
        manifest=ManifestReference(
            bucket="hidden",
            object_key="hidden",
            byte_length=1,
            media_type="hidden",
            sha256=b"x" * 32,
        ),
        contract_sha256=b"y" * 32,
    )