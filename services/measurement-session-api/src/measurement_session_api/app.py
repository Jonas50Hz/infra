"""Anonymous read-only HTTP surface for finalized measurement sessions."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import base64
import json
import logging
from pathlib import PurePosixPath
import re
from typing import Any, Protocol
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse

from measurement_session_common.manifest import ArtifactDescriptor, SessionManifest
from measurement_session_api.catalog import CatalogCursor, CatalogSession, CatalogStore
from measurement_session_api.storage import (
    ArtifactNotFoundError,
    ArtifactStream,
    IncompleteMeasurementDataError,
    ObjectStoreError,
)

LOGGER = logging.getLogger(__name__)
VERIFIED_SESSION_SCAN_BATCH_SIZE = 100


class CatalogWorkerProtocol(Protocol):
    """Minimal worker behavior needed by the API lifecycle and health route."""

    failure: str | None

    @property
    def ready(self) -> bool:
        """Report whether Kafka catalog materialization is connected."""

    def start(self) -> None:
        """Start consuming finalized-session records."""

    def stop(self) -> None:
        """Stop consuming finalized-session records."""


class SessionStoreProtocol(Protocol):
    """Object-store behavior deliberately kept inside the API process."""

    def verified_manifest(self, session: CatalogSession) -> SessionManifest:
        """Return a validated manifest for the cataloged session."""

    def artifact(self, session: CatalogSession, artifact_id: str) -> tuple[Any, ArtifactStream]:
        """Return a verified artifact descriptor and streaming body."""


def create_app(
    catalog: CatalogStore,
    session_store: SessionStoreProtocol,
    worker: CatalogWorkerProtocol,
) -> FastAPI:
    """Create an API with no mutation routes, auth routes, or object-store URLs."""

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        catalog.initialize()
        worker.start()
        try:
            yield
        finally:
            worker.stop()

    app = FastAPI(
        title="WAMA Measurement Sessions",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
        lifespan=lifespan,
    )

    @app.get("/healthz")
    def health() -> dict[str, str]:
        if worker.failure or not worker.ready:
            raise HTTPException(status_code=503, detail="Catalog materialization is unavailable")
        return {"status": "ok"}

    @app.get("/v1/measurement-sessions")
    def list_measurement_sessions(
        limit: int = Query(default=25, ge=1, le=100),
        cursor: str | None = Query(default=None),
    ) -> dict[str, Any]:
        after = _decode_cursor(cursor) if cursor else None
        try:
            page, next_cursor = _verified_session_page(catalog, session_store, limit, after)
        except ObjectStoreError as error:
            LOGGER.warning("Session catalog verification is unavailable: %s", error)
            raise HTTPException(status_code=503, detail="Session verification is unavailable") from error
        return {"items": [_summary(session) for session in page], "next_cursor": next_cursor}

    @app.get("/v1/measurement-sessions/{session_id}")
    def measurement_session_detail(session_id: str) -> dict[str, Any]:
        session = _session_or_404(catalog, session_id)
        try:
            manifest = session_store.verified_manifest(session)
        except ObjectStoreError as error:
            LOGGER.warning("Refusing detail for %s after manifest verification failure: %s", session_id, error)
            raise HTTPException(status_code=503, detail="Session artifact verification is unavailable") from error
        return {
            **_detail(session),
            "artifacts": [
                {
                    "id": artifact.artifact_id,
                    "content_type": artifact.content_type,
                    "size_bytes": artifact.size_bytes,
                    "download_name": _download_filename(session, artifact),
                    "download_url": f"/v1/measurement-sessions/{session.session_id}/artifacts/{artifact.artifact_id}",
                }
                for artifact in manifest.artifacts
            ],
        }

    @app.get("/v1/measurement-sessions/{session_id}/artifacts/{artifact_id}")
    def measurement_session_artifact(session_id: str, artifact_id: str) -> StreamingResponse:
        session = _session_or_404(catalog, session_id)
        try:
            descriptor, stream = session_store.artifact(session, artifact_id)
        except ArtifactNotFoundError as error:
            raise HTTPException(status_code=404, detail="Artifact not found") from error
        except ObjectStoreError as error:
            LOGGER.warning("Refusing download for %s/%s after verification failure: %s", session_id, artifact_id, error)
            raise HTTPException(status_code=503, detail="Artifact verification is unavailable") from error
        return StreamingResponse(
            _stream_object(stream),
            media_type=stream.content_type,
            headers={
                "Cache-Control": "no-store",
                "Content-Disposition": f'attachment; filename="{_download_filename(session, descriptor)}"',
                "Content-Length": str(stream.size_bytes),
                "X-Content-Type-Options": "nosniff",
            },
        )

    return app


def _session_or_404(catalog: CatalogStore, session_id: str) -> CatalogSession:
    try:
        parsed = UUID(session_id)
    except ValueError as error:
        raise HTTPException(status_code=404, detail="Measurement session not found") from error
    if str(parsed) != session_id:
        raise HTTPException(status_code=404, detail="Measurement session not found")
    session = catalog.get(session_id)
    if session is None:
        raise HTTPException(status_code=404, detail="Measurement session not found")
    return session


def _summary(session: CatalogSession) -> dict[str, Any]:
    return {
        "session_id": session.session_id,
        "source_mrid": session.source_mrid,
        "started_at": _timestamp_text(session.started_at),
        "ended_at": _timestamp_text(session.ended_at),
        "finalized_at": _timestamp_text(session.finalized_at),
        "measurement_count": session.measurement_count,
        "artifact_count": session.artifact_count,
    }


def _verified_session_page(
    catalog: CatalogStore,
    session_store: SessionStoreProtocol,
    limit: int,
    after: CatalogCursor | None,
) -> tuple[tuple[CatalogSession, ...], str | None]:
    """Page only sessions whose waveform covers all declared measurements."""

    verified: list[CatalogSession] = []
    scan_after = after
    while len(verified) <= limit:
        candidates = tuple(catalog.list_sessions(VERIFIED_SESSION_SCAN_BATCH_SIZE, scan_after))
        if not candidates:
            break
        for session in candidates:
            try:
                session_store.verified_manifest(session)
            except IncompleteMeasurementDataError as error:
                LOGGER.warning(
                    "Hiding incomplete MeasurementSession %s: %s",
                    session.session_id,
                    error,
                )
                continue
            verified.append(session)
            if len(verified) > limit:
                break
        if len(verified) > limit or len(candidates) < VERIFIED_SESSION_SCAN_BATCH_SIZE:
            break
        last_candidate = candidates[-1]
        scan_after = CatalogCursor(
            finalized_at=last_candidate.finalized_at,
            session_id=last_candidate.session_id,
        )

    page = tuple(verified[:limit])
    next_cursor = _encode_cursor(page[-1]) if len(verified) > limit and page else None
    return page, next_cursor


def _detail(session: CatalogSession) -> dict[str, Any]:
    return {
        **_summary(session),
        "metadata": [{"key": key, "value": value} for key, value in session.metadata],
    }


def _download_filename(session: CatalogSession, artifact: ArtifactDescriptor) -> str:
    """Build an ASCII attachment name from public session fields only."""

    source = re.sub(r"[^A-Za-z0-9]+", "-", session.source_mrid).strip("-").lower()
    source = source[:80] or "measurement-session"
    extension = PurePosixPath(artifact.object_key).suffix.lower()
    if not re.fullmatch(r"\.[a-z0-9]{1,16}", extension):
        extension = ".bin"
    started_on = session.started_at.astimezone(timezone.utc).date().isoformat()
    return f"{source}_{started_on}_{artifact.artifact_id}{extension}"


def _timestamp_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _encode_cursor(session: CatalogSession) -> str:
    document = {"finalized_at": _timestamp_text(session.finalized_at), "session_id": session.session_id}
    return base64.urlsafe_b64encode(json.dumps(document, separators=(",", ":")).encode("utf-8")).decode("ascii").rstrip("=")


def _decode_cursor(value: str) -> CatalogCursor:
    try:
        padding = "=" * (-len(value) % 4)
        document = json.loads(base64.urlsafe_b64decode(value + padding).decode("utf-8"))
        if not isinstance(document, dict) or set(document) != {"finalized_at", "session_id"}:
            raise ValueError("unsupported cursor fields")
        session_id = document["session_id"]
        finalized_at = document["finalized_at"]
        if not isinstance(session_id, str) or not isinstance(finalized_at, str):
            raise ValueError("cursor values are malformed")
        if str(UUID(session_id)) != session_id:
            raise ValueError("cursor session ID is malformed")
        timestamp = datetime.fromisoformat(finalized_at.replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            raise ValueError("cursor timestamp has no timezone")
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise HTTPException(status_code=400, detail="Cursor is invalid") from error
    return CatalogCursor(finalized_at=timestamp.astimezone(timezone.utc), session_id=session_id)


def _stream_object(stream: ArtifactStream) -> Iterator[bytes]:
    try:
        while chunk := stream.body.read(64 * 1024):
            yield chunk
    finally:
        stream.body.close()