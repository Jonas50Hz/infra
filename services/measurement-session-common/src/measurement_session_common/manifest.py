"""Canonical JSON manifest encoding for immutable session artifacts."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any

from measurement_session_common.contract import (
    ContractValidationError,
    MAX_ARTIFACTS,
    MAX_MANIFEST_BYTES,
    validate_artifact_descriptor,
)


@dataclass(frozen=True)
class ArtifactDescriptor:
    """One immutable artifact described by a finalized-session manifest."""

    artifact_id: str
    object_key: str
    content_type: str
    size_bytes: int
    sha256_hex: str


@dataclass(frozen=True)
class SessionManifest:
    """Validated canonical manifest retrieved from object storage."""

    session_id: str
    artifacts: tuple[ArtifactDescriptor, ...]


def canonical_manifest_bytes(session_id: str, artifacts: tuple[ArtifactDescriptor, ...]) -> bytes:
    """Encode a deterministic manifest whose bytes are SHA-256 addressed."""

    _validate_manifest(session_id, artifacts)
    document = {
        "artifacts": [
            {
                "content_type": artifact.content_type,
                "id": artifact.artifact_id,
                "object_key": artifact.object_key,
                "sha256": artifact.sha256_hex,
                "size_bytes": artifact.size_bytes,
            }
            for artifact in artifacts
        ],
        "session_id": session_id,
        "version": 1,
    }
    return json.dumps(document, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


def parse_manifest(payload: bytes, expected_session_id: str) -> SessionManifest:
    """Parse and validate a canonical manifest before exposing artifact metadata."""

    if not payload or len(payload) > MAX_MANIFEST_BYTES:
        raise ContractValidationError(f"manifest must contain at most {MAX_MANIFEST_BYTES} bytes")
    try:
        document = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ContractValidationError("manifest is not valid UTF-8 JSON") from error
    if not isinstance(document, dict) or set(document) != {"artifacts", "session_id", "version"}:
        raise ContractValidationError("manifest has unsupported fields")
    if document["version"] != 1 or document["session_id"] != expected_session_id:
        raise ContractValidationError("manifest session identity or version does not match")
    raw_artifacts = document["artifacts"]
    if not isinstance(raw_artifacts, list):
        raise ContractValidationError("manifest artifacts must be a list")

    artifacts: list[ArtifactDescriptor] = []
    for raw_artifact in raw_artifacts:
        artifacts.append(_artifact_from_document(expected_session_id, raw_artifact))
    result = tuple(artifacts)
    _validate_manifest(expected_session_id, result)
    if canonical_manifest_bytes(expected_session_id, result) != payload:
        raise ContractValidationError("manifest is not canonically encoded")
    return SessionManifest(session_id=expected_session_id, artifacts=result)


def _artifact_from_document(session_id: str, value: Any) -> ArtifactDescriptor:
    if not isinstance(value, dict) or set(value) != {
        "content_type",
        "id",
        "object_key",
        "sha256",
        "size_bytes",
    }:
        raise ContractValidationError("manifest artifact has unsupported fields")
    artifact_id = value["id"]
    object_key = value["object_key"]
    content_type = value["content_type"]
    size_bytes = value["size_bytes"]
    sha256_hex = value["sha256"]
    if not all(isinstance(item, str) for item in (artifact_id, object_key, content_type, sha256_hex)):
        raise ContractValidationError("manifest artifact strings are malformed")
    if not isinstance(size_bytes, int) or isinstance(size_bytes, bool):
        raise ContractValidationError("manifest artifact size_bytes must be an integer")
    validate_artifact_descriptor(
        session_id,
        artifact_id,
        object_key,
        content_type,
        size_bytes,
        sha256_hex,
    )
    return ArtifactDescriptor(
        artifact_id=artifact_id,
        object_key=object_key,
        content_type=content_type,
        size_bytes=size_bytes,
        sha256_hex=sha256_hex,
    )


def _validate_manifest(session_id: str, artifacts: tuple[ArtifactDescriptor, ...]) -> None:
    if not 0 < len(artifacts) <= MAX_ARTIFACTS:
        raise ContractValidationError(f"manifest must contain between 1 and {MAX_ARTIFACTS} artifacts")
    previous_id: str | None = None
    for artifact in artifacts:
        validate_artifact_descriptor(
            session_id,
            artifact.artifact_id,
            artifact.object_key,
            artifact.content_type,
            artifact.size_bytes,
            artifact.sha256_hex,
        )
        if previous_id is not None and artifact.artifact_id <= previous_id:
            raise ContractValidationError("manifest artifacts must be strictly sorted by unique id")
        previous_id = artifact.artifact_id