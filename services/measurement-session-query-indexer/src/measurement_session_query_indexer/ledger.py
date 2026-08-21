"""Mutable registration ledger for immutable Blobmeta-to-Iceberg work."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import psycopg

from measurement_session_query_indexer.artifact import VerifiedArtifact


class LedgerError(RuntimeError):
    """Raised when the index ledger cannot reconcile immutable evidence."""


class LedgerConflictError(LedgerError):
    """Raised when a registered Blobmeta identity changes immutable evidence."""


RegistrationCallback = Callable[[VerifiedArtifact], object]


@dataclass(frozen=True)
class Registration:
    """Ledger evidence retained for one successfully registered artifact."""

    blob_id: str
    session_id: str
    object_uri: str
    sha256: bytes
    byte_length: int
    measurement_count: int


class PostgresRegistrationLedger:
    """Serialize registration by blob ID and recover after at-least-once replay."""

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn

    def initialize(self) -> None:
        """Create only the mutable schema owned by this query indexer."""

        with psycopg.connect(self._dsn, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.execute("CREATE SCHEMA IF NOT EXISTS session_query_index;")
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS session_query_index.registrations (
                        blob_id text PRIMARY KEY,
                        session_id uuid NOT NULL,
                        object_uri text NOT NULL,
                        object_sha256 bytea NOT NULL CHECK (octet_length(object_sha256) = 32),
                        object_byte_length bigint NOT NULL CHECK (object_byte_length > 0),
                        measurement_count bigint NOT NULL CHECK (measurement_count >= 0),
                        registered_at timestamptz NOT NULL DEFAULT now()
                    );
                    """
                )
                cursor.execute(
                    "GRANT USAGE ON SCHEMA session_query_index TO trino_blobmeta_reader;"
                )
                cursor.execute(
                    "GRANT SELECT ON ALL TABLES IN SCHEMA session_query_index "
                    "TO trino_blobmeta_reader;"
                )
                cursor.execute(
                    "ALTER DEFAULT PRIVILEGES IN SCHEMA session_query_index "
                    "GRANT SELECT ON TABLES TO trino_blobmeta_reader;"
                )

    def register(self, artifact: VerifiedArtifact, callback: RegistrationCallback) -> bool:
        """Reconcile the exact Iceberg file then record its immutable identity."""

        with psycopg.connect(self._dsn, connect_timeout=5) as connection:
            with connection.transaction():
                with connection.cursor() as cursor:
                    cursor.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0));",
                        (artifact.blob_id,),
                    )
                    cursor.execute(
                        """
                        SELECT blob_id, session_id, object_uri, object_sha256,
                               object_byte_length, measurement_count
                        FROM session_query_index.registrations
                        WHERE blob_id = %s;
                        """,
                        (artifact.blob_id,),
                    )
                    existing = cursor.fetchone()
                    if existing is not None:
                        _validate_registration(existing, artifact)
                        callback(artifact)
                        return False
                    callback(artifact)
                    cursor.execute(
                        """
                        INSERT INTO session_query_index.registrations (
                            blob_id, session_id, object_uri, object_sha256,
                            object_byte_length, measurement_count
                        ) VALUES (%s, %s, %s, %s, %s, %s);
                        """,
                        (
                            artifact.blob_id,
                            artifact.session_id,
                            artifact.object_uri,
                            artifact.sha256,
                            artifact.byte_length,
                            artifact.measurement_count,
                        ),
                    )
                    return True


def _validate_registration(row: tuple[object, ...], artifact: VerifiedArtifact) -> None:
    registration = Registration(
        blob_id=str(row[0]),
        session_id=str(row[1]),
        object_uri=str(row[2]),
        sha256=bytes(row[3]),
        byte_length=int(row[4]),
        measurement_count=int(row[5]),
    )
    if registration != Registration(
        blob_id=artifact.blob_id,
        session_id=artifact.session_id,
        object_uri=artifact.object_uri,
        sha256=artifact.sha256,
        byte_length=artifact.byte_length,
        measurement_count=artifact.measurement_count,
    ):
        raise LedgerConflictError(
            f"Session query registration {artifact.blob_id} conflicts with immutable evidence"
        )