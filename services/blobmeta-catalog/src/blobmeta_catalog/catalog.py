"""Immutable relational projection of compacted raw-Protobuf Blobmeta records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
import json
from typing import Any, Protocol

import psycopg

from measurement_session_common.contract import validate_blobmeta
from measurement_session_common.generated.blobmeta_pb2 import Blobmeta


class CatalogError(RuntimeError):
    """Raised when the metadata projection cannot preserve immutable evidence."""


class CatalogConflictError(CatalogError):
    """Raised when a compacted identity is replayed with divergent raw bytes."""


@dataclass(frozen=True)
class ObjectReference:
    """Optional immutable Parquet object pointer in one Blobmeta result."""

    bucket: str
    object_key: str
    media_type: str
    byte_length: int
    sha256: bytes


@dataclass(frozen=True)
class CatalogBlob:
    """A validated Blobmeta record ready for one PostgreSQL transaction."""

    blob_id: str
    session_id: str
    request_sha256: bytes
    requested_at: datetime
    started_at: datetime
    ended_at: datetime
    finalized_at: datetime
    mrids: tuple[tuple[str, int], ...]
    measurement_count: int
    status: str
    rejection_reason: str | None
    metadata: tuple[tuple[str, str], ...]
    object: ObjectReference | None
    contract_sha256: bytes

    @classmethod
    def from_protobuf(cls, message: Blobmeta, payload: bytes) -> "CatalogBlob":
        """Validate and translate raw Kafka bytes without mutating the evidence."""

        validate_blobmeta(message)
        object_reference = None
        if message.HasField("object"):
            object_reference = ObjectReference(
                bucket=message.object.bucket,
                object_key=message.object.object_key,
                media_type=message.object.media_type,
                byte_length=message.object.byte_length,
                sha256=bytes(message.object.sha256),
            )
        status_names = {
            Blobmeta.COMPLETE: "COMPLETE",
            Blobmeta.PARTIAL: "PARTIAL",
            Blobmeta.REJECTED: "REJECTED",
        }
        return cls(
            blob_id=message.blob_id,
            session_id=message.session_id,
            request_sha256=bytes(message.request_sha256),
            requested_at=message.requested_at.ToDatetime(tzinfo=timezone.utc),
            started_at=message.started_at.ToDatetime(tzinfo=timezone.utc),
            ended_at=message.ended_at.ToDatetime(tzinfo=timezone.utc),
            finalized_at=message.finalized_at.ToDatetime(tzinfo=timezone.utc),
            mrids=tuple(
                (coverage.mrid, coverage.measurement_count)
                for coverage in message.mrid_coverage
            ),
            measurement_count=message.measurement_count,
            status=status_names[message.status],
            rejection_reason=message.rejection_reason or None,
            metadata=tuple((entry.key, entry.value) for entry in message.metadata),
            object=object_reference,
            contract_sha256=sha256(payload).digest(),
        )


class CatalogStore(Protocol):
    """Storage boundary used by the Kafka consumer and process startup."""

    def initialize(self) -> None:
        """Create app-owned schemas and tables when missing."""

    def insert(self, blob: CatalogBlob) -> None:
        """Insert a Blobmeta projection or prove an exact replay."""


class PostgresCatalog:
    """PostgreSQL implementation of the immutable Blobmeta projection."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def initialize(self) -> None:
        """Create only the schema owned by this compacted-topic materializer."""

        with psycopg.connect(self._dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute("CREATE SCHEMA IF NOT EXISTS blobmeta_catalog;")
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS blobmeta_catalog.session_blobs (
                        blob_id text PRIMARY KEY,
                        session_id uuid NOT NULL,
                        request_sha256 bytea NOT NULL CHECK (octet_length(request_sha256) = 32),
                        requested_at timestamptz NOT NULL,
                        started_at timestamptz NOT NULL,
                        ended_at timestamptz NOT NULL,
                        finalized_at timestamptz NOT NULL,
                        measurement_count bigint NOT NULL CHECK (measurement_count >= 0),
                        status text NOT NULL CHECK (status IN ('COMPLETE', 'PARTIAL', 'REJECTED')),
                        rejection_reason text,
                        metadata jsonb NOT NULL,
                        object_bucket text,
                        object_key text,
                        object_media_type text,
                        object_byte_length bigint,
                        object_sha256 bytea,
                        contract_sha256 bytea NOT NULL CHECK (octet_length(contract_sha256) = 32),
                        created_at timestamptz NOT NULL DEFAULT now(),
                        CHECK (started_at < ended_at),
                        CHECK (finalized_at >= requested_at),
                        CHECK (
                            (status = 'REJECTED'
                                AND rejection_reason IS NOT NULL
                                AND object_bucket IS NULL
                                AND object_key IS NULL
                                AND object_media_type IS NULL
                                AND object_byte_length IS NULL
                                AND object_sha256 IS NULL)
                            OR
                            (status IN ('COMPLETE', 'PARTIAL')
                                AND rejection_reason IS NULL
                                AND object_bucket IS NOT NULL
                                AND object_key IS NOT NULL
                                AND object_media_type IS NOT NULL
                                AND object_byte_length > 0
                                AND octet_length(object_sha256) = 32)
                        )
                    );
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS blobmeta_catalog.session_blob_mrids (
                        blob_id text NOT NULL REFERENCES blobmeta_catalog.session_blobs (blob_id),
                        mrid text NOT NULL,
                        measurement_count bigint NOT NULL CHECK (measurement_count >= 0),
                        PRIMARY KEY (blob_id, mrid)
                    );
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS session_blobs_session_idx
                    ON blobmeta_catalog.session_blobs (session_id, finalized_at DESC);
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS session_blobs_status_idx
                    ON blobmeta_catalog.session_blobs (status, finalized_at DESC);
                    """
                )
                cursor.execute(
                    """
                    CREATE INDEX IF NOT EXISTS session_blob_mrids_mrid_idx
                    ON blobmeta_catalog.session_blob_mrids (mrid, blob_id);
                    """
                )
                cursor.execute(
                    """
                    CREATE OR REPLACE FUNCTION blobmeta_catalog.reject_mutation()
                    RETURNS trigger
                    LANGUAGE plpgsql
                    AS $$
                    BEGIN
                        RAISE EXCEPTION 'Blobmeta catalog rows are immutable';
                    END;
                    $$;
                    """
                )
                for table_name in ("session_blobs", "session_blob_mrids"):
                    cursor.execute(
                        f"DROP TRIGGER IF EXISTS {table_name}_immutable "
                        f"ON blobmeta_catalog.{table_name};"
                    )
                    cursor.execute(
                        f"""
                        CREATE TRIGGER {table_name}_immutable
                        BEFORE UPDATE OR DELETE ON blobmeta_catalog.{table_name}
                        FOR EACH ROW EXECUTE FUNCTION blobmeta_catalog.reject_mutation();
                        """
                    )

    def insert(self, blob: CatalogBlob) -> None:
        """Commit a projection and coverage rows before the Kafka offset advances."""

        with psycopg.connect(self._dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO blobmeta_catalog.session_blobs (
                        blob_id, session_id, request_sha256,
                        requested_at, started_at, ended_at, finalized_at,
                        measurement_count, status, rejection_reason, metadata,
                        object_bucket, object_key, object_media_type,
                        object_byte_length, object_sha256, contract_sha256
                    ) VALUES (
                        %(blob_id)s, %(session_id)s, %(request_sha256)s,
                        %(requested_at)s, %(started_at)s, %(ended_at)s, %(finalized_at)s,
                        %(measurement_count)s, %(status)s, %(rejection_reason)s, %(metadata)s::jsonb,
                        %(object_bucket)s, %(object_key)s, %(object_media_type)s,
                        %(object_byte_length)s, %(object_sha256)s, %(contract_sha256)s
                    ) ON CONFLICT (blob_id) DO NOTHING
                    RETURNING contract_sha256;
                    """,
                    _insert_values(blob),
                )
                inserted = cursor.fetchone()
                if inserted is None:
                    cursor.execute(
                        """
                        SELECT contract_sha256
                        FROM blobmeta_catalog.session_blobs
                        WHERE blob_id = %s;
                        """,
                        (blob.blob_id,),
                    )
                    existing = cursor.fetchone()
                    if existing is None or bytes(existing[0]) != blob.contract_sha256:
                        raise CatalogConflictError(
                            f"Blobmeta {blob.blob_id} conflicts with an immutable catalog row"
                        )
                    return
                if blob.mrids:
                    cursor.executemany(
                        """
                        INSERT INTO blobmeta_catalog.session_blob_mrids (
                            blob_id, mrid, measurement_count
                        ) VALUES (%s, %s, %s);
                        """,
                        [
                            (blob.blob_id, mrid, count)
                            for mrid, count in blob.mrids
                        ],
                    )


def _insert_values(blob: CatalogBlob) -> dict[str, Any]:
    object_reference = blob.object
    return {
        "blob_id": blob.blob_id,
        "session_id": blob.session_id,
        "request_sha256": blob.request_sha256,
        "requested_at": blob.requested_at,
        "started_at": blob.started_at,
        "ended_at": blob.ended_at,
        "finalized_at": blob.finalized_at,
        "measurement_count": blob.measurement_count,
        "status": blob.status,
        "rejection_reason": blob.rejection_reason,
        "metadata": json.dumps(
            [{"key": key, "value": value} for key, value in blob.metadata],
            separators=(",", ":"),
        ),
        "object_bucket": object_reference.bucket if object_reference else None,
        "object_key": object_reference.object_key if object_reference else None,
        "object_media_type": object_reference.media_type if object_reference else None,
        "object_byte_length": object_reference.byte_length if object_reference else None,
        "object_sha256": object_reference.sha256 if object_reference else None,
        "contract_sha256": blob.contract_sha256,
    }