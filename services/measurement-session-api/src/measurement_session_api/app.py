"""HTTP and browser-confirmed submission surface for MeasurementSession."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from uuid import uuid4

from fastapi import FastAPI, HTTPException, status
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from measurement_session_common.contract import (
    ContractValidationError,
    successful_blob_id,
    validate_measurement_session_request,
)
from measurement_session_common.generated.measurement_session_pb2 import MeasurementSessionRequest
from measurement_session_api.config import Settings
from measurement_session_api.publisher import SessionPublishError, SessionPublisher


class SessionSubmission(BaseModel):
    """Browser or API input before it is normalized to the Kafka contract."""

    session_id: str | None = None
    started_at: datetime
    ended_at: datetime
    mrids: list[str] = Field(default_factory=list)
    capture_reason: str | None = None


class SessionAccepted(BaseModel):
    """The immutable identity and expected result location of one request."""

    session_id: str
    blob_id: str
    requested_at: datetime
    session_dashboard_url: str


def create_app(
    settings: Settings,
    publisher: SessionPublisher,
    clock: Callable[[], datetime] | None = None,
    static_root: Path | None = None,
) -> FastAPI:
    """Create the bounded request API and optional same-origin confirmation UI."""

    now = (lambda: datetime.now(timezone.utc)) if clock is None else clock

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            yield
        finally:
            publisher.close()

    app = FastAPI(
        title="WAMA Measurement Session API",
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )

    @app.get("/healthz")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.post(
        "/v1/measurement-sessions",
        response_model=SessionAccepted,
        status_code=status.HTTP_202_ACCEPTED,
    )
    async def submit(submission: SessionSubmission) -> SessionAccepted:
        try:
            request = build_request(submission, settings, now())
        except (ContractValidationError, ValueError) as error:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(error)) from error

        try:
            publisher.publish(request)
        except SessionPublishError as error:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)) from error

        started_at = request.started_at.ToDatetime(tzinfo=timezone.utc)
        ended_at = request.ended_at.ToDatetime(tzinfo=timezone.utc)
        return SessionAccepted(
            session_id=request.session_id,
            blob_id=successful_blob_id(request.session_id),
            requested_at=request.requested_at.ToDatetime(tzinfo=timezone.utc),
            session_dashboard_url=_session_dashboard_url(
                settings.grafana_session_dashboard_url,
                request.session_id,
                started_at,
                ended_at,
            ),
        )

    if static_root is not None:
        app.mount("/", StaticFiles(directory=static_root, html=True), name="static")

    return app


def build_request(
    submission: SessionSubmission,
    settings: Settings,
    requested_at: datetime,
) -> MeasurementSessionRequest:
    """Normalize browser selection into one validated immutable request."""

    request = MeasurementSessionRequest(
        session_id=submission.session_id or str(uuid4()),
        mrids=sorted({mrid.strip() for mrid in submission.mrids if mrid.strip()}),
    )
    request.requested_at.FromDatetime(_utc(requested_at, "requested_at"))
    request.started_at.FromDatetime(_utc(submission.started_at, "started_at"))
    request.ended_at.FromDatetime(_utc(submission.ended_at, "ended_at"))

    metadata = {"request_origin": "grafana"}
    capture_reason = (submission.capture_reason or "").strip()
    if capture_reason:
        metadata["capture_reason"] = capture_reason
    for key, value in sorted(metadata.items()):
        entry = request.metadata.add()
        entry.key = key
        entry.value = value

    validate_measurement_session_request(
        request,
        max_mrids=settings.max_mrids,
        max_interval_hours=settings.max_interval_hours,
    )
    return request


def _utc(value: datetime, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must include a UTC offset")
    return value.astimezone(timezone.utc)


def _session_dashboard_url(
    base_url: str,
    session_id: str,
    started_at: datetime,
    ended_at: datetime,
) -> str:
    query = urlencode(
        {
            "var-blob_id": successful_blob_id(session_id),
            "from": _milliseconds(started_at),
            "to": _milliseconds(ended_at),
        }
    )
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}{query}"


def _milliseconds(value: datetime) -> int:
    return int(value.astimezone(timezone.utc).timestamp() * 1_000)