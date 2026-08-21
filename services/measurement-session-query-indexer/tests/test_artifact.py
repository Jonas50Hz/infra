"""Tests for immutable v2 session artifact verification."""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from measurement_session_common.contract import (
    PARQUET_MEDIA_TYPE,
    SESSION_PARQUET_FIELDS,
    SESSION_PARQUET_SCHEMA_VERSION,
)
from measurement_session_common.generated.blobmeta_pb2 import Blobmeta
from measurement_session_query_indexer.artifact import ArtifactVerificationError, verify_artifact


class ArtifactVerificationTests(unittest.TestCase):
    """Prove only matching canonical Parquet evidence is accepted."""

    def test_verifies_a_matching_v2_artifact(self) -> None:
        payload = _parquet_payload()
        result = _result(payload)

        artifact = verify_artifact(_S3(payload), result)

        self.assertEqual(artifact.object_uri, "s3://wama-measurement-sessions/sessions/test/measurements.parquet")
        self.assertEqual(artifact.measurement_count, 1)
        self.assertEqual(artifact.coverage, (("urn:wama:poc:a", 1),))

    def test_rejects_parquet_identity_that_differs_from_blobmeta(self) -> None:
        payload = _parquet_payload(blob_id="sessions/other/measurements")

        with self.assertRaisesRegex(ArtifactVerificationError, "identity"):
            verify_artifact(_S3(payload), _result(payload))


class _S3:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def head_object(self, **kwargs: object) -> dict[str, object]:
        del kwargs
        return {
            "ContentLength": len(self._payload),
            "ContentType": PARQUET_MEDIA_TYPE,
            "Metadata": {"sha256": sha256(self._payload).hexdigest()},
        }

    def get_object(self, **kwargs: object) -> dict[str, object]:
        del kwargs
        return {"Body": BytesIO(self._payload)}


def _result(payload: bytes) -> Blobmeta:
    result = Blobmeta(
        blob_id="sessions/test/measurements",
        session_id="4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237",
        measurement_count=1,
        status=Blobmeta.COMPLETE,
    )
    coverage = result.mrid_coverage.add()
    coverage.mrid = "urn:wama:poc:a"
    coverage.measurement_count = 1
    result.object.bucket = "wama-measurement-sessions"
    result.object.object_key = "sessions/test/measurements.parquet"
    result.object.media_type = PARQUET_MEDIA_TYPE
    result.object.parquet_schema_version = SESSION_PARQUET_SCHEMA_VERSION
    result.object.byte_length = len(payload)
    result.object.sha256 = sha256(payload).digest()
    return result


def _parquet_payload(blob_id: str = "sessions/test/measurements") -> bytes:
    with TemporaryDirectory() as directory:
        path = Path(directory) / "artifact.parquet"
        schema = pa.schema(
            [
                pa.field(
                    name,
                    _data_type(name),
                    nullable=name not in {"blob_id", "session_id", "timestamp_mccs", "mrid", "value_type"},
                    metadata={b"PARQUET:field_id": str(field_id).encode("ascii")},
                )
                for name, field_id in SESSION_PARQUET_FIELDS
            ],
            metadata={b"wama.parquet.schema_version": str(SESSION_PARQUET_SCHEMA_VERSION).encode("ascii")},
        )
        pq.write_table(
            pa.Table.from_pylist(
                [
                    {
                        "blob_id": blob_id,
                        "session_id": "4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237",
                        "timestamp_mccs": datetime(2026, 8, 21, 12, tzinfo=timezone.utc),
                        "mrid": "urn:wama:poc:a",
                        "value_type": "double",
                        "double_value": 50.01,
                    }
                ],
                schema=schema,
            ),
            path,
            compression="zstd",
        )
        return path.read_bytes()


def _data_type(name: str) -> pa.DataType:
    if name in {"blob_id", "session_id", "mrid", "value_type", "string_value"}:
        return pa.string()
    if name == "double_value":
        return pa.float64()
    if name in {"int_value", "uint_value"}:
        return pa.int64()
    if name in {
        "quality_valid",
        "quality_substituted",
        "quality_operator_blocked",
        "quality_overflow",
        "quality_old_data",
        "bool_value",
    }:
        return pa.bool_()
    return pa.timestamp("us", tz="UTC")