"""Tests for manual-commit Blobmeta query indexing decisions."""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from types import SimpleNamespace
import unittest

from google.protobuf.timestamp_pb2 import Timestamp

from measurement_session_common.contract import (
    PARQUET_MEDIA_TYPE,
    SESSION_PARQUET_SCHEMA_VERSION,
    rejected_blob_id,
    session_parquet_key,
    successful_blob_id,
)
from measurement_session_common.generated.blobmeta_pb2 import Blobmeta
from measurement_session_query_indexer.artifact import VerifiedArtifact
from measurement_session_query_indexer.config import Settings
from measurement_session_query_indexer.consumer import BlobmetaQueryIndexer


class QueryIndexerTests(unittest.TestCase):
    """Ensure only successful artifacts reach the mutable registration ledger."""

    def test_registers_a_valid_completed_blobmeta(self) -> None:
        result = _completed()
        verifier = _Verifier(_artifact(result))
        ledger = _Ledger()
        writer = _Writer()
        indexer = BlobmetaQueryIndexer(Settings.from_environment({}), verifier, ledger, writer)

        registered = indexer.process_record(_record(result))

        self.assertTrue(registered)
        self.assertEqual(verifier.calls, [result.blob_id])
        self.assertEqual(ledger.calls, [result.blob_id])
        self.assertEqual(writer.calls, [result.blob_id])

    def test_skips_a_valid_rejected_blobmeta(self) -> None:
        result = _rejected()
        verifier = _Verifier(_artifact(_completed()))
        ledger = _Ledger()
        writer = _Writer()
        indexer = BlobmetaQueryIndexer(Settings.from_environment({}), verifier, ledger, writer)

        registered = indexer.process_record(_record(result))

        self.assertFalse(registered)
        self.assertEqual(verifier.calls, [])
        self.assertEqual(ledger.calls, [])
        self.assertEqual(writer.calls, [])


class _Verifier:
    def __init__(self, artifact: VerifiedArtifact) -> None:
        self._artifact = artifact
        self.calls: list[str] = []

    def __call__(self, result: Blobmeta) -> VerifiedArtifact:
        self.calls.append(result.blob_id)
        return self._artifact


class _Ledger:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def register(self, artifact: VerifiedArtifact, callback) -> bool:
        self.calls.append(artifact.blob_id)
        callback(artifact)
        return True


class _Writer:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def ensure_registered(self, artifact: VerifiedArtifact) -> None:
        self.calls.append(artifact.blob_id)


def _completed() -> Blobmeta:
    session_id = "4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237"
    result = Blobmeta(
        blob_id=successful_blob_id(session_id),
        session_id=session_id,
        request_sha256=b"x" * 32,
        mrids=("urn:wama:poc:a",),
        measurement_count=1,
        status=Blobmeta.COMPLETE,
    )
    _timestamps(result)
    coverage = result.mrid_coverage.add()
    coverage.mrid = "urn:wama:poc:a"
    coverage.measurement_count = 1
    result.object.bucket = "wama-measurement-sessions"
    result.object.object_key = session_parquet_key(session_id)
    result.object.media_type = PARQUET_MEDIA_TYPE
    result.object.parquet_schema_version = SESSION_PARQUET_SCHEMA_VERSION
    result.object.byte_length = 1234
    result.object.sha256 = sha256(b"artifact").digest()
    return result


def _rejected() -> Blobmeta:
    session_id = "6b8d7d9c-47d9-4f8c-a3c5-7c1c4f988f2e"
    digest = b"r" * 32
    result = Blobmeta(
        blob_id=rejected_blob_id(session_id, digest),
        session_id=session_id,
        request_sha256=digest,
        status=Blobmeta.REJECTED,
        rejection_reason="fixture rejection",
    )
    _timestamps(result)
    return result


def _timestamps(result: Blobmeta) -> None:
    values = {
        "requested_at": datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc),
        "started_at": datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc),
        "ended_at": datetime(2026, 8, 21, 12, 1, tzinfo=timezone.utc),
        "finalized_at": datetime(2026, 8, 21, 12, 2, tzinfo=timezone.utc),
    }
    for name, value in values.items():
        destination: Timestamp = getattr(result, name)
        destination.FromDatetime(value)


def _record(result: Blobmeta) -> SimpleNamespace:
    return SimpleNamespace(
        key=result.blob_id.encode("utf-8"),
        value=result.SerializeToString(deterministic=True),
    )


def _artifact(result: Blobmeta) -> VerifiedArtifact:
    return VerifiedArtifact(
        blob_id=result.blob_id,
        session_id=result.session_id,
        object_uri=f"s3://{result.object.bucket}/{result.object.object_key}",
        byte_length=result.object.byte_length,
        sha256=bytes(result.object.sha256),
        measurement_count=result.measurement_count,
        coverage=tuple(
            (coverage.mrid, coverage.measurement_count)
            for coverage in result.mrid_coverage
        ),
    )