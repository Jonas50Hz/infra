"""Upload immutable session artifacts and publish their raw-Protobuf contract."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import timezone
from hashlib import sha256
from pathlib import Path
from typing import Any, BinaryIO

from botocore.exceptions import ClientError
from google.protobuf.timestamp_pb2 import Timestamp

from measurement_session_common.contract import (
    DEFAULT_SESSION_BUCKET,
    MANIFEST_MEDIA_TYPE,
    ContractValidationError,
    validate_measurement_session,
)
from measurement_session_common.generated.measurement_session_pb2 import MeasurementSession
from measurement_session_common.manifest import ArtifactDescriptor, canonical_manifest_bytes
from measurement_session_common.measurement_csv import (
    MeasurementSeriesValidationError,
    is_measurement_csv_artifact,
    validate_measurement_csv,
)
from measurement_session_exporter.config import FinalizedSessionFixture


@dataclass(frozen=True)
class ExportResult:
    """Evidence produced by one successful final-session export."""

    payload: bytes
    session: MeasurementSession


def export_finalized_session(
    fixture: FinalizedSessionFixture,
    s3_client: Any,
    producer: Any,
    kafka_topic: str,
    bucket: str = DEFAULT_SESSION_BUCKET,
) -> ExportResult:
    """Write immutable artifacts and publish one raw-Protobuf final session."""

    if bucket != DEFAULT_SESSION_BUCKET:
        raise ContractValidationError(f"Session exports must use {DEFAULT_SESSION_BUCKET!r}")
    artifacts = _artifact_descriptors(fixture)
    descriptors = tuple(artifact for artifact, _ in artifacts)
    manifest_payload = canonical_manifest_bytes(fixture.session_id, descriptors)
    _validate_measurement_artifacts(fixture, artifacts)
    manifest_key = f"sessions/{fixture.session_id}/manifest.json"
    manifest_digest = sha256(manifest_payload).digest()
    for artifact, source_path in artifacts:
        _put_or_verify_file(
            s3_client,
            bucket,
            artifact.object_key,
            source_path,
            artifact.content_type,
            artifact.sha256_hex,
            artifact.size_bytes,
        )

    _put_or_verify_bytes(
        s3_client,
        bucket,
        manifest_key,
        manifest_payload,
        MANIFEST_MEDIA_TYPE,
        manifest_digest.hex(),
    )

    session = _session_message(fixture, manifest_key, manifest_payload, manifest_digest)
    validate_measurement_session(session)
    payload = session.SerializeToString()
    producer.send(
        kafka_topic,
        key=session.session_id.encode("utf-8"),
        value=payload,
        timestamp_ms=_timestamp_milliseconds(session.finalized_at),
    ).get(timeout=30)
    return ExportResult(payload=payload, session=session)


def _artifact_descriptors(
    fixture: FinalizedSessionFixture,
) -> tuple[tuple[ArtifactDescriptor, Path], ...]:
    artifacts: list[tuple[ArtifactDescriptor, Path]] = []
    for configured in sorted(fixture.artifacts, key=lambda artifact: artifact.artifact_id):
        suffix = configured.path.suffix.lower() or ".bin"
        object_key = f"sessions/{fixture.session_id}/artifacts/{configured.artifact_id}{suffix}"
        digest, size_bytes = _file_digest(configured.path)
        artifacts.append(
            (
                ArtifactDescriptor(
                    artifact_id=configured.artifact_id,
                    object_key=object_key,
                    content_type=configured.content_type,
                    size_bytes=size_bytes,
                    sha256_hex=digest,
                ),
                configured.path,
            )
        )
    return tuple(artifacts)


def _validate_measurement_artifacts(
    fixture: FinalizedSessionFixture,
    artifacts: tuple[tuple[ArtifactDescriptor, Path], ...],
) -> None:
    """Reject a final session unless its waveform contains every declared value."""

    waveform_artifacts = tuple(
        artifact
        for artifact in artifacts
        if is_measurement_csv_artifact(artifact[0].artifact_id, artifact[0].content_type)
    )
    if len(waveform_artifacts) != 1:
        raise ContractValidationError(
            "finalized session must contain exactly one text/csv waveform artifact"
        )
    descriptor, source_path = waveform_artifacts[0]
    try:
        with source_path.open(encoding="utf-8", newline="") as source:
            validate_measurement_csv(
                source,
                fixture.measurement_count,
                fixture.started_at,
                fixture.ended_at,
            )
    except (OSError, UnicodeError, csv.Error, MeasurementSeriesValidationError) as error:
        raise ContractValidationError(
            "waveform artifact does not contain every finalized-session measurement"
        ) from error


def _session_message(
    fixture: FinalizedSessionFixture,
    manifest_key: str,
    manifest_payload: bytes,
    manifest_digest: bytes,
) -> MeasurementSession:
    session = MeasurementSession(
        session_id=fixture.session_id,
        source_mrid=fixture.source_mrid,
        measurement_count=fixture.measurement_count,
        artifact_count=len(fixture.artifacts),
    )
    session.started_at.CopyFrom(_protobuf_timestamp(fixture.started_at))
    session.ended_at.CopyFrom(_protobuf_timestamp(fixture.ended_at))
    session.finalized_at.CopyFrom(_protobuf_timestamp(fixture.finalized_at))
    for key, value in sorted(fixture.metadata):
        entry = session.metadata.add()
        entry.key = key
        entry.value = value
    session.manifest.bucket = DEFAULT_SESSION_BUCKET
    session.manifest.object_key = manifest_key
    session.manifest.byte_length = len(manifest_payload)
    session.manifest.media_type = MANIFEST_MEDIA_TYPE
    session.manifest.sha256 = manifest_digest
    return session


def _put_or_verify_file(
    client: Any,
    bucket: str,
    object_key: str,
    path: Path,
    content_type: str,
    digest: str,
    size_bytes: int,
) -> None:
    if _object_exists(client, bucket, object_key):
        _verify_existing_object(client, bucket, object_key, content_type, digest, size_bytes)
        return
    with path.open("rb") as body:
        client.put_object(
            Bucket=bucket,
            Key=object_key,
            Body=body,
            ContentType=content_type,
            Metadata={"sha256": digest},
        )
    _verify_object_metadata(client, bucket, object_key, content_type, digest, size_bytes)


def _put_or_verify_bytes(
    client: Any,
    bucket: str,
    object_key: str,
    payload: bytes,
    content_type: str,
    digest: str,
) -> None:
    if _object_exists(client, bucket, object_key):
        _verify_existing_object(client, bucket, object_key, content_type, digest, len(payload))
        return
    client.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=payload,
        ContentType=content_type,
        Metadata={"sha256": digest},
    )
    _verify_object_metadata(client, bucket, object_key, content_type, digest, len(payload))


def _object_exists(client: Any, bucket: str, object_key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=object_key)
    except ClientError as error:
        code = str(error.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise
    return True


def _verify_existing_object(
    client: Any,
    bucket: str,
    object_key: str,
    content_type: str,
    digest: str,
    size_bytes: int,
) -> None:
    _verify_object_metadata(client, bucket, object_key, content_type, digest, size_bytes)
    response = client.get_object(Bucket=bucket, Key=object_key)
    body: BinaryIO = response["Body"]
    hasher = sha256()
    total_size = 0
    try:
        while chunk := body.read(64 * 1024):
            hasher.update(chunk)
            total_size += len(chunk)
    finally:
        body.close()
    if total_size != size_bytes or hasher.hexdigest() != digest:
        raise ContractValidationError(f"Existing object {object_key!r} does not match immutable export data")


def _verify_object_metadata(
    client: Any,
    bucket: str,
    object_key: str,
    content_type: str,
    digest: str,
    size_bytes: int,
) -> None:
    response = client.head_object(Bucket=bucket, Key=object_key)
    metadata = response.get("Metadata", {})
    if (
        response.get("ContentLength") != size_bytes
        or response.get("ContentType") != content_type
        or metadata.get("sha256") != digest
    ):
        raise ContractValidationError(f"Object metadata does not match immutable export data: {object_key}")


def _file_digest(path: Path) -> tuple[str, int]:
    hasher = sha256()
    size_bytes = 0
    with path.open("rb") as source:
        while chunk := source.read(64 * 1024):
            hasher.update(chunk)
            size_bytes += len(chunk)
    return hasher.hexdigest(), size_bytes


def _protobuf_timestamp(value: Any) -> Timestamp:
    timestamp = Timestamp()
    timestamp.FromDatetime(value.astimezone(timezone.utc))
    return timestamp


def _timestamp_milliseconds(value: Timestamp) -> int:
    return value.seconds * 1_000 + value.nanos // 1_000_000