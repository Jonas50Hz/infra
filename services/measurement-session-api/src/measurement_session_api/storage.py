"""Integrity-checked SeaweedFS access kept behind the catalog API."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from hashlib import sha256
from io import TextIOWrapper
from typing import Any, BinaryIO

from botocore.exceptions import BotoCoreError, ClientError

from measurement_session_common.contract import ContractValidationError, MAX_MANIFEST_BYTES
from measurement_session_common.manifest import ArtifactDescriptor, SessionManifest, parse_manifest
from measurement_session_common.measurement_csv import (
    MeasurementSeriesValidationError,
    is_measurement_csv_artifact,
    validate_measurement_csv,
)
from measurement_session_api.catalog import CatalogSession


class ObjectStoreError(RuntimeError):
    """Raised when object storage cannot prove a cataloged artifact is intact."""


class ArtifactNotFoundError(ObjectStoreError):
    """Raised when a verified manifest does not contain the requested artifact."""


class IncompleteMeasurementDataError(ObjectStoreError):
    """Raised when a waveform does not cover its declared finalized session."""


@dataclass(frozen=True)
class ArtifactStream:
    """A verified object-store stream held exclusively by the API response."""

    body: BinaryIO
    content_type: str
    size_bytes: int


class SeaweedSessionStore:
    """Read manifests and artifacts only after checking their immutable evidence."""

    def __init__(self, client: Any) -> None:
        self._client = client

    def verified_manifest(self, session: CatalogSession) -> SessionManifest:
        """Fetch, hash, and validate the manifest referenced by the Kafka contract."""

        reference = session.manifest
        head = self._head(reference.bucket, reference.object_key)
        metadata = head.get("Metadata", {})
        if (
            head.get("ContentLength") != reference.byte_length
            or head.get("ContentType") != reference.media_type
            or metadata.get("sha256") != reference.sha256.hex()
        ):
            raise ObjectStoreError("Manifest object metadata does not match the immutable contract")
        if reference.byte_length > MAX_MANIFEST_BYTES:
            raise ObjectStoreError("Manifest exceeds the finalized-session size limit")
        payload = self._read_small_object(reference.bucket, reference.object_key)
        if len(payload) != reference.byte_length or sha256(payload).digest() != reference.sha256:
            raise ObjectStoreError("Manifest bytes do not match the immutable contract")
        try:
            manifest = parse_manifest(payload, session.session_id)
        except ContractValidationError as error:
            raise ObjectStoreError("Manifest does not satisfy the finalized-session contract") from error
        if len(manifest.artifacts) != session.artifact_count:
            raise ObjectStoreError("Manifest artifact count does not match the finalized-session contract")
        self._verify_measurement_series(session, manifest)
        return manifest

    def artifact(self, session: CatalogSession, artifact_id: str) -> tuple[ArtifactDescriptor, ArtifactStream]:
        """Return one verified artifact stream selected only by a logical manifest ID."""

        manifest = self.verified_manifest(session)
        descriptor = next(
            (artifact for artifact in manifest.artifacts if artifact.artifact_id == artifact_id),
            None,
        )
        if descriptor is None:
            raise ArtifactNotFoundError("Artifact is not present in the finalized-session manifest")
        self._verify_artifact_metadata(session.manifest.bucket, descriptor)
        try:
            response = self._client.get_object(Bucket=session.manifest.bucket, Key=descriptor.object_key)
        except (BotoCoreError, ClientError, OSError) as error:
            raise ObjectStoreError("Artifact could not be retrieved from object storage") from error
        return descriptor, ArtifactStream(
            body=response["Body"],
            content_type=descriptor.content_type,
            size_bytes=descriptor.size_bytes,
        )

    def _verify_measurement_series(
        self,
        session: CatalogSession,
        manifest: SessionManifest,
    ) -> None:
        """Require the PoC waveform CSV to cover every declared measurement."""

        if session.measurement_count == 0:
            return
        waveforms = tuple(
            artifact
            for artifact in manifest.artifacts
            if is_measurement_csv_artifact(artifact.artifact_id, artifact.content_type)
        )
        if len(waveforms) != 1:
            raise IncompleteMeasurementDataError(
                "Finalized session does not contain exactly one measurement waveform CSV"
            )
        descriptor = waveforms[0]
        self._verify_artifact_metadata(session.manifest.bucket, descriptor)
        try:
            response = self._client.get_object(
                Bucket=session.manifest.bucket,
                Key=descriptor.object_key,
            )
            body: BinaryIO = response["Body"]
        except (BotoCoreError, ClientError, OSError) as error:
            raise ObjectStoreError("Measurement waveform could not be retrieved from object storage") from error
        try:
            with TextIOWrapper(body, encoding="utf-8", newline="") as source:
                validate_measurement_csv(
                    source,
                    session.measurement_count,
                    session.started_at,
                    session.ended_at,
                )
        except (UnicodeError, csv.Error, MeasurementSeriesValidationError) as error:
            raise IncompleteMeasurementDataError(
                "Measurement waveform does not contain every finalized-session value"
            ) from error

    def _verify_artifact_metadata(
        self,
        bucket: str,
        descriptor: ArtifactDescriptor,
    ) -> None:
        """Check the immutable manifest's metadata evidence for one artifact."""

        head = self._head(bucket, descriptor.object_key)
        metadata = head.get("Metadata", {})
        if (
            head.get("ContentLength") != descriptor.size_bytes
            or head.get("ContentType") != descriptor.content_type
            or metadata.get("sha256") != descriptor.sha256_hex
        ):
            raise ObjectStoreError("Artifact object metadata does not match its verified manifest")

    def _head(self, bucket: str, object_key: str) -> dict[str, Any]:
        try:
            return self._client.head_object(Bucket=bucket, Key=object_key)
        except (BotoCoreError, ClientError, OSError) as error:
            raise ObjectStoreError("Object storage metadata could not be retrieved") from error

    def _read_small_object(self, bucket: str, object_key: str) -> bytes:
        try:
            response = self._client.get_object(Bucket=bucket, Key=object_key)
            body: BinaryIO = response["Body"]
        except (BotoCoreError, ClientError, OSError) as error:
            raise ObjectStoreError("Object storage data could not be retrieved") from error
        try:
            payload = body.read(MAX_MANIFEST_BYTES + 1)
        finally:
            body.close()
        if len(payload) > MAX_MANIFEST_BYTES:
            raise ObjectStoreError("Manifest exceeds the finalized-session size limit")
        return payload