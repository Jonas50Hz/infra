"""Stream and validate immutable v2 session Parquet artifacts from SeaweedFS."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, BinaryIO

from botocore.exceptions import BotoCoreError, ClientError
import pyarrow as pa
import pyarrow.parquet as pq

from measurement_session_common.contract import (
    PARQUET_MEDIA_TYPE,
    SESSION_PARQUET_FIELDS,
    SESSION_PARQUET_SCHEMA_VERSION,
)
from measurement_session_common.generated.blobmeta_pb2 import Blobmeta


class ArtifactVerificationError(RuntimeError):
    """Raised when a Blobmeta artifact cannot be proven safe to register."""


@dataclass(frozen=True)
class VerifiedArtifact:
    """Immutable evidence that matches one canonical Iceberg data file."""

    blob_id: str
    session_id: str
    object_uri: str
    byte_length: int
    sha256: bytes
    measurement_count: int
    coverage: tuple[tuple[str, int], ...]


_EXPECTED_TYPES = {
    "blob_id": "string",
    "session_id": "string",
    "timestamp_mccs": "timestamp[us, tz=UTC]",
    "mrid": "string",
    "value_type": "string",
    "double_value": "double",
    "int_value": "int64",
    "uint_value": "int64",
    "bool_value": "bool",
    "string_value": "string",
    "timestamp_value": "timestamp[us, tz=UTC]",
    "timestamp_field": "timestamp[us, tz=UTC]",
    "timestamp_gateway": "timestamp[us, tz=UTC]",
    "quality_valid": "bool",
    "quality_substituted": "bool",
    "quality_operator_blocked": "bool",
    "quality_overflow": "bool",
    "quality_old_data": "bool",
}
_REQUIRED_FIELDS = {"blob_id", "session_id", "timestamp_mccs", "mrid", "value_type"}


def verify_artifact(client: Any, result: Blobmeta) -> VerifiedArtifact:
    """Download, hash, and validate the exact Parquet object named by Blobmeta."""

    if not result.HasField("object"):
        raise ArtifactVerificationError("Completed Blobmeta has no Parquet object")
    reference = result.object
    if reference.parquet_schema_version != SESSION_PARQUET_SCHEMA_VERSION:
        raise ArtifactVerificationError("Blobmeta has an unsupported Parquet schema version")
    expected_digest = bytes(reference.sha256)
    try:
        head = client.head_object(Bucket=reference.bucket, Key=reference.object_key)
    except (BotoCoreError, ClientError, OSError) as error:
        raise ArtifactVerificationError("Unable to read session artifact metadata") from error
    if (
        head.get("ContentLength") != reference.byte_length
        or head.get("ContentType") != PARQUET_MEDIA_TYPE
        or head.get("Metadata", {}).get("sha256") != expected_digest.hex()
    ):
        raise ArtifactVerificationError("Session artifact metadata does not match Blobmeta")

    with TemporaryDirectory(prefix="wama-session-index-") as directory:
        path = Path(directory) / "measurements.parquet"
        actual_digest, actual_length = _download(client, reference.bucket, reference.object_key, path)
        if actual_length != reference.byte_length or actual_digest != expected_digest:
            raise ArtifactVerificationError("Session artifact bytes do not match Blobmeta")
        _verify_parquet(path, result)

    return VerifiedArtifact(
        blob_id=result.blob_id,
        session_id=result.session_id,
        object_uri=f"s3://{reference.bucket}/{reference.object_key}",
        byte_length=reference.byte_length,
        sha256=expected_digest,
        measurement_count=result.measurement_count,
        coverage=tuple(
            (item.mrid, item.measurement_count)
            for item in result.mrid_coverage
        ),
    )


def _download(client: Any, bucket: str, object_key: str, destination: Path) -> tuple[bytes, int]:
    try:
        response = client.get_object(Bucket=bucket, Key=object_key)
        body: BinaryIO = response["Body"]
        digest = sha256()
        byte_length = 0
        try:
            with destination.open("wb") as output:
                while chunk := body.read(64 * 1024):
                    output.write(chunk)
                    digest.update(chunk)
                    byte_length += len(chunk)
        finally:
            body.close()
    except (BotoCoreError, ClientError, OSError, KeyError) as error:
        raise ArtifactVerificationError("Unable to download session artifact") from error
    return digest.digest(), byte_length


def _verify_parquet(path: Path, result: Blobmeta) -> None:
    try:
        parquet = pq.ParquetFile(path)
        schema = parquet.schema_arrow
    except (OSError, ValueError, pa.ArrowException) as error:
        raise ArtifactVerificationError("Session artifact is not readable Parquet") from error
    expected_names = tuple(name for name, _ in SESSION_PARQUET_FIELDS)
    actual_names = tuple(field.name for field in schema)
    if actual_names != expected_names:
        raise ArtifactVerificationError("Session Parquet columns do not match schema v2")
    if schema.metadata.get(b"wama.parquet.schema_version") != str(
        SESSION_PARQUET_SCHEMA_VERSION
    ).encode("ascii"):
        raise ArtifactVerificationError("Session Parquet schema version does not match Blobmeta")
    for name, field_id in SESSION_PARQUET_FIELDS:
        field = schema.field(name)
        metadata = field.metadata or {}
        if metadata.get(b"PARQUET:field_id") != str(field_id).encode("ascii"):
            raise ArtifactVerificationError(f"Session Parquet field ID is invalid for {name}")
        if str(field.type) != _EXPECTED_TYPES[name]:
            raise ArtifactVerificationError(f"Session Parquet type is invalid for {name}")
        if field.nullable == (name in _REQUIRED_FIELDS):
            raise ArtifactVerificationError(f"Session Parquet nullability is invalid for {name}")

    expected_coverage = {
        item.mrid: item.measurement_count
        for item in result.mrid_coverage
    }
    actual_coverage: Counter[str] = Counter()
    count = 0
    try:
        for batch in parquet.iter_batches(columns=["blob_id", "session_id", "mrid"]):
            blob_ids = batch.column(0).to_pylist()
            session_ids = batch.column(1).to_pylist()
            mrids = batch.column(2).to_pylist()
            for blob_id, session_id, mrid in zip(blob_ids, session_ids, mrids, strict=True):
                if blob_id != result.blob_id or session_id != result.session_id:
                    raise ArtifactVerificationError("Session Parquet row identity does not match Blobmeta")
                if mrid not in expected_coverage:
                    raise ArtifactVerificationError("Session Parquet contains an unexpected MRID")
                actual_coverage[mrid] += 1
                count += 1
    except ArtifactVerificationError:
        raise
    except (OSError, ValueError, pa.ArrowException) as error:
        raise ArtifactVerificationError("Unable to scan session Parquet rows") from error
    normalized_coverage = tuple(
        (item.mrid, actual_coverage[item.mrid])
        for item in result.mrid_coverage
    )
    if count != result.measurement_count or normalized_coverage != tuple(
        (item.mrid, item.measurement_count)
        for item in result.mrid_coverage
    ):
        raise ArtifactVerificationError("Session Parquet row coverage does not match Blobmeta")