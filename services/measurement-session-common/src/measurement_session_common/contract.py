"""Validation rules that make finalized MeasurementSession records immutable."""

from __future__ import annotations

from datetime import timezone
import re
from uuid import UUID

from measurement_session_common.generated.measurement_session_pb2 import MeasurementSession

DEFAULT_SESSION_BUCKET = "wama-measurement-sessions"
MANIFEST_MEDIA_TYPE = "application/vnd.wama.measurement-session-manifest+json;version=1"
MAX_ARTIFACTS = 64
MAX_ARTIFACT_BYTES = 4 * 1024 * 1024 * 1024
MAX_MANIFEST_BYTES = 1024 * 1024
MAX_METADATA_ENTRIES = 32
MAX_METADATA_KEY_BYTES = 64
MAX_METADATA_VALUE_BYTES = 256
MAX_SERIALIZED_SESSION_BYTES = 16 * 1024
MAX_SOURCE_MRID_BYTES = 256

METADATA_KEY_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_.-]{0,63}\Z")
OBJECT_KEY_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,511}\Z")


class ContractValidationError(ValueError):
    """Raised when a session violates the immutable finalized-session contract."""


def validate_measurement_session(session: MeasurementSession) -> None:
    """Validate a raw-Protobuf final session before storage or cataloging."""

    _canonical_uuid(session.session_id, "session_id")
    _bounded_text(session.source_mrid, "source_mrid", MAX_SOURCE_MRID_BYTES)

    timestamps = []
    for field_name in ("started_at", "ended_at", "finalized_at"):
        if not session.HasField(field_name):
            raise ContractValidationError(f"{field_name} is required")
        timestamp = getattr(session, field_name)
        try:
            timestamp.ToDatetime(tzinfo=timezone.utc)
        except ValueError as error:
            raise ContractValidationError(f"{field_name} is outside the Protobuf timestamp range") from error
        timestamps.append(timestamp.seconds * 1_000_000_000 + timestamp.nanos)
    if timestamps[0] > timestamps[1] or timestamps[1] > timestamps[2]:
        raise ContractValidationError("started_at must be <= ended_at <= finalized_at")

    if len(session.metadata) > MAX_METADATA_ENTRIES:
        raise ContractValidationError(f"metadata must contain at most {MAX_METADATA_ENTRIES} entries")
    previous_key: str | None = None
    for entry in session.metadata:
        if not METADATA_KEY_PATTERN.fullmatch(entry.key):
            raise ContractValidationError("metadata keys must use ASCII letters, digits, dots, underscores, or hyphens")
        _bounded_text(entry.value, f"metadata.{entry.key}", MAX_METADATA_VALUE_BYTES)
        if previous_key is not None and entry.key <= previous_key:
            raise ContractValidationError("metadata entries must be strictly sorted by unique key")
        previous_key = entry.key

    if session.artifact_count == 0 or session.artifact_count > MAX_ARTIFACTS:
        raise ContractValidationError(f"artifact_count must be between 1 and {MAX_ARTIFACTS}")
    if not session.HasField("manifest"):
        raise ContractValidationError("manifest is required")
    _validate_manifest_reference(session)

    if session.ByteSize() > MAX_SERIALIZED_SESSION_BYTES:
        raise ContractValidationError(
            f"serialized session must not exceed {MAX_SERIALIZED_SESSION_BYTES} bytes"
        )


def validate_artifact_descriptor(
    session_id: str,
    artifact_id: str,
    object_key: str,
    content_type: str,
    size_bytes: int,
    sha256_hex: str,
) -> None:
    """Validate one manifest artifact without trusting caller-supplied paths."""

    _canonical_uuid(session_id, "session_id")
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", artifact_id):
        raise ContractValidationError("artifact ids must use lowercase letters, digits, and hyphens")
    expected_prefix = f"sessions/{session_id}/artifacts/"
    if not object_key.startswith(expected_prefix):
        raise ContractValidationError("artifact object_key is outside its session namespace")
    _safe_object_key(object_key, "artifact object_key")
    if not _is_media_type(content_type):
        raise ContractValidationError("artifact content_type must be a media type")
    if not 0 < size_bytes <= MAX_ARTIFACT_BYTES:
        raise ContractValidationError(f"artifact size_bytes must be between 1 and {MAX_ARTIFACT_BYTES}")
    if not re.fullmatch(r"[0-9a-f]{64}", sha256_hex):
        raise ContractValidationError("artifact sha256 must be lowercase hexadecimal SHA-256")


def _validate_manifest_reference(session: MeasurementSession) -> None:
    manifest = session.manifest
    if manifest.bucket != DEFAULT_SESSION_BUCKET:
        raise ContractValidationError(f"manifest.bucket must be {DEFAULT_SESSION_BUCKET!r}")
    expected_key = f"sessions/{session.session_id}/manifest.json"
    if manifest.object_key != expected_key:
        raise ContractValidationError("manifest.object_key must use the canonical session namespace")
    _safe_object_key(manifest.object_key, "manifest.object_key")
    if manifest.byte_length == 0 or manifest.byte_length > MAX_MANIFEST_BYTES:
        raise ContractValidationError(
            f"manifest.byte_length must be between 1 and {MAX_MANIFEST_BYTES}"
        )
    if manifest.media_type != MANIFEST_MEDIA_TYPE:
        raise ContractValidationError("manifest.media_type is not the canonical manifest media type")
    if len(manifest.sha256) != 32:
        raise ContractValidationError("manifest.sha256 must contain exactly 32 bytes")


def _canonical_uuid(value: str, field_name: str) -> None:
    try:
        parsed = UUID(value)
    except (TypeError, ValueError) as error:
        raise ContractValidationError(f"{field_name} must be a canonical UUID") from error
    if str(parsed) != value:
        raise ContractValidationError(f"{field_name} must be a lowercase canonical UUID")


def _bounded_text(value: str, field_name: str, maximum_bytes: int) -> None:
    if not value:
        raise ContractValidationError(f"{field_name} must not be empty")
    if len(value.encode("utf-8")) > maximum_bytes:
        raise ContractValidationError(f"{field_name} must not exceed {maximum_bytes} UTF-8 bytes")


def _safe_object_key(value: str, field_name: str) -> None:
    if not OBJECT_KEY_PATTERN.fullmatch(value):
        raise ContractValidationError(f"{field_name} contains unsupported characters")
    if any(part in {"", ".", ".."} for part in value.split("/")):
        raise ContractValidationError(f"{field_name} must not contain empty or traversal path segments")


def _is_media_type(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9!#$&^_.+-]+/[A-Za-z0-9!#$&^_.+-]+", value))