"""Immutable PostgreSQL projection for raw-Protobuf finalized sessions."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
from typing import Any, Protocol

import psycopg

from measurement_session_common.contract import validate_measurement_session
from measurement_session_common.generated.measurement_session_pb2 import MeasurementSession


class CatalogError(RuntimeError):
    """Raised when the catalog cannot safely materialize a session."""


class CatalogConflictError(CatalogError):
    """Raised when the same session ID is replayed with different bytes."""


@dataclass(frozen=True)
class ManifestReference:
    """Cataloged immutable reference to the canonical object-store manifest."""

    bucket: str
    object_key: str
    byte_length: int
    media_type: str
    sha256: bytes


@dataclass(frozen=True)
class CatalogSession:
    """A finalized raw-Protobuf record materialized without mutable fields."""

    session_id: str
    source_mrid: str
    started_at: datetime
    ended_at: datetime
    finalized_at: datetime
    metadata: tuple[tuple[str, str], ...]
    measurement_count: int
    artifact_count: int
    manifest: ManifestReference
    contract_sha256: bytes

    @classmethod
    def from_protobuf(cls, message: MeasurementSession, payload: bytes) -> "CatalogSession":
        """Validate and translate raw Kafka bytes into immutable catalog fields."""

        validate_measurement_session(message)
        return cls(
            session_id=message.session_id,
            source_mrid=message.source_mrid,
            started_at=message.started_at.ToDatetime(tzinfo=timezone.utc),
            ended_at=message.ended_at.ToDatetime(tzinfo=timezone.utc),
            finalized_at=message.finalized_at.ToDatetime(tzinfo=timezone.utc),
            metadata=tuple((entry.key, entry.value) for entry in message.metadata),
            measurement_count=message.measurement_count,
            artifact_count=message.artifact_count,
            manifest=ManifestReference(
                bucket=message.manifest.bucket,
                object_key=message.manifest.object_key,
                byte_length=message.manifest.byte_length,
                media_type=message.manifest.media_type,
                sha256=bytes(message.manifest.sha256),
            ),
            contract_sha256=sha256(payload).digest(),
        )


@dataclass(frozen=True)
class CatalogCursor:
    """Stable order position for finalized-session pagination."""

    finalized_at: datetime
    session_id: str


class CatalogStore(Protocol):
    """Storage abstraction used by the Kafka materializer and HTTP API."""

    def initialize(self) -> None:
        """Create catalog structures required by the API."""

    def insert(self, session: CatalogSession) -> None:
        """Insert a session or prove an existing row is an identical replay."""

    def get(self, session_id: str) -> CatalogSession | None:
        """Return one immutable session by its canonical ID."""

    def list_sessions(self, limit: int, after: CatalogCursor | None) -> Sequence[CatalogSession]:
        """List finalized sessions in stable descending finalization order."""


class PostgresCatalog:
    """PostgreSQL implementation of the immutable session catalog."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def initialize(self) -> None:
        """Create a separate app-owned schema without touching Kafka mirrors."""

        with psycopg.connect(self._dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute("CREATE SCHEMA IF NOT EXISTS measurement_session_catalog;")
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS measurement_session_catalog.measurement_sessions (
                        session_id uuid PRIMARY KEY,
                        source_mrid text NOT NULL,
                        started_at timestamptz NOT NULL,
                        ended_at timestamptz NOT NULL,
                        finalized_at timestamptz NOT NULL,
                        metadata jsonb NOT NULL,
                        measurement_count bigint NOT NULL CHECK (measurement_count >= 0),
                        artifact_count integer NOT NULL CHECK (artifact_count > 0),
                        manifest_bucket text NOT NULL,
                        manifest_object_key text NOT NULL,
                        manifest_byte_length bigint NOT NULL CHECK (manifest_byte_length > 0),
                        manifest_media_type text NOT NULL,
                        manifest_sha256 bytea NOT NULL CHECK (octet_length(manifest_sha256) = 32),
                        contract_sha256 bytea NOT NULL CHECK (octet_length(contract_sha256) = 32),
                        created_at timestamptz NOT NULL DEFAULT now(),
                        CHECK (started_at <= ended_at AND ended_at <= finalized_at)
                    );
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS measurement_sessions_finalized_cursor_idx
                    ON measurement_session_catalog.measurement_sessions
                    (finalized_at DESC, session_id DESC);
                    """
                )
                cursor.execute(
                    """
                    CREATE OR REPLACE FUNCTION measurement_session_catalog.reject_mutation()
                    RETURNS trigger
                    LANGUAGE plpgsql
                    AS $$
                    BEGIN
                        RAISE EXCEPTION 'measurement session catalog rows are immutable';
                    END;
                    $$;
                    """
                )
                cursor.execute(
                    "DROP TRIGGER IF EXISTS measurement_sessions_immutable "
                    "ON measurement_session_catalog.measurement_sessions;"
                )
                cursor.execute(
                    """
                    CREATE TRIGGER measurement_sessions_immutable
                    BEFORE UPDATE OR DELETE ON measurement_session_catalog.measurement_sessions
                    FOR EACH ROW EXECUTE FUNCTION measurement_session_catalog.reject_mutation();
                    """
                )

    def insert(self, session: CatalogSession) -> None:
        """Commit a new projection row or reject divergent at-least-once replay."""

        with psycopg.connect(self._dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO measurement_session_catalog.measurement_sessions (
                        session_id, source_mrid, started_at, ended_at, finalized_at,
                        metadata, measurement_count, artifact_count,
                        manifest_bucket, manifest_object_key, manifest_byte_length,
                        manifest_media_type, manifest_sha256, contract_sha256
                    ) VALUES (
                        %(session_id)s, %(source_mrid)s, %(started_at)s, %(ended_at)s, %(finalized_at)s,
                        %(metadata)s::jsonb, %(measurement_count)s, %(artifact_count)s,
                        %(manifest_bucket)s, %(manifest_object_key)s, %(manifest_byte_length)s,
                        %(manifest_media_type)s, %(manifest_sha256)s, %(contract_sha256)s
                    ) ON CONFLICT (session_id) DO NOTHING
                    RETURNING contract_sha256;
                    """,
                    _insert_values(session),
                )
                inserted = cursor.fetchone()
                if inserted is not None:
                    return
                cursor.execute(
                    """
                    SELECT contract_sha256
                    FROM measurement_session_catalog.measurement_sessions
                    WHERE session_id = %s;
                    """,
                    (session.session_id,),
                )
                existing = cursor.fetchone()
                if existing is None or bytes(existing[0]) != session.contract_sha256:
                    raise CatalogConflictError(
                        f"MeasurementSession {session.session_id} conflicts with an immutable catalog row"
                    )

    def get(self, session_id: str) -> CatalogSession | None:
        """Load one catalog row without disclosing its object-store location."""

        with psycopg.connect(self._dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(_select_sql("WHERE session_id = %s"), (session_id,))
                row = cursor.fetchone()
        return _session_from_row(row) if row is not None else None

    def list_sessions(self, limit: int, after: CatalogCursor | None) -> Sequence[CatalogSession]:
        """Return a bounded ordered page of immutable catalog rows."""

        clause = ""
        parameters: tuple[Any, ...]
        if after is None:
            parameters = (limit,)
        else:
            clause = "WHERE (finalized_at, session_id) < (%s, %s::uuid)"
            parameters = (after.finalized_at, after.session_id, limit)
        with psycopg.connect(self._dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(_select_sql(clause) + " LIMIT %s;", parameters)
                rows = cursor.fetchall()
        return tuple(_session_from_row(row) for row in rows)


def _insert_values(session: CatalogSession) -> dict[str, Any]:
    return {
        "session_id": session.session_id,
        "source_mrid": session.source_mrid,
        "started_at": session.started_at,
        "ended_at": session.ended_at,
        "finalized_at": session.finalized_at,
        "metadata": json.dumps(
            [{"key": key, "value": value} for key, value in session.metadata],
            separators=(",", ":"),
        ),
        "measurement_count": session.measurement_count,
        "artifact_count": session.artifact_count,
        "manifest_bucket": session.manifest.bucket,
        "manifest_object_key": session.manifest.object_key,
        "manifest_byte_length": session.manifest.byte_length,
        "manifest_media_type": session.manifest.media_type,
        "manifest_sha256": session.manifest.sha256,
        "contract_sha256": session.contract_sha256,
    }


def _select_sql(where_clause: str) -> str:
    return f"""
        SELECT
            session_id::text,
            source_mrid,
            started_at,
            ended_at,
            finalized_at,
            metadata,
            measurement_count,
            artifact_count,
            manifest_bucket,
            manifest_object_key,
            manifest_byte_length,
            manifest_media_type,
            manifest_sha256,
            contract_sha256
        FROM measurement_session_catalog.measurement_sessions
        {where_clause}
        ORDER BY finalized_at DESC, session_id DESC
    """


def _session_from_row(row: Sequence[Any]) -> CatalogSession:
    raw_metadata = row[5]
    if not isinstance(raw_metadata, list):
        raise CatalogError("Catalog metadata is malformed")
    metadata: list[tuple[str, str]] = []
    for entry in raw_metadata:
        if not isinstance(entry, dict) or not isinstance(entry.get("key"), str) or not isinstance(entry.get("value"), str):
            raise CatalogError("Catalog metadata entry is malformed")
        metadata.append((entry["key"], entry["value"]))
    return CatalogSession(
        session_id=str(row[0]),
        source_mrid=str(row[1]),
        started_at=row[2].astimezone(timezone.utc),
        ended_at=row[3].astimezone(timezone.utc),
        finalized_at=row[4].astimezone(timezone.utc),
        metadata=tuple(metadata),
        measurement_count=int(row[6]),
        artifact_count=int(row[7]),
        manifest=ManifestReference(
            bucket=str(row[8]),
            object_key=str(row[9]),
            byte_length=int(row[10]),
            media_type=str(row[11]),
            sha256=bytes(row[12]),
        ),
        contract_sha256=bytes(row[13]),
    )