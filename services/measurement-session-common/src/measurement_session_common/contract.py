"""Validation rules for immutable session requests and Blobmeta results."""

from __future__ import annotations

from datetime import timedelta, timezone
from hashlib import sha256
import re
from uuid import UUID

from measurement_session_common.generated.blobmeta_pb2 import Blobmeta
from measurement_session_common.generated.measurement_session_pb2 import MeasurementSessionRequest

DEFAULT_SESSION_BUCKET = "wama-measurement-sessions"
PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"
SESSION_PARQUET_SCHEMA_VERSION = 2
SESSION_PARQUET_FIELDS = (
    ("blob_id", 1),
    ("session_id", 2),
    ("timestamp_mccs", 3),
    ("mrid", 4),
    ("value_type", 5),
    ("double_value", 6),
    ("int_value", 7),
    ("uint_value", 8),
    ("bool_value", 9),
    ("string_value", 10),
    ("timestamp_value", 11),
    ("timestamp_field", 12),
    ("timestamp_gateway", 13),
    ("quality_valid", 14),
    ("quality_substituted", 15),
    ("quality_operator_blocked", 16),
    ("quality_overflow", 17),
    ("quality_old_data", 18),
)
SESSION_PARQUET_FIELD_IDS = dict(SESSION_PARQUET_FIELDS)
BLOBMETA_MEDIA_TYPE = "application/vnd.wama.blobmeta+protobuf;version=1"
MAX_METADATA_ENTRIES = 32
MAX_METADATA_VALUE_BYTES = 256
MAX_MRID_BYTES = 256
MAX_REQUEST_SERIALIZED_BYTES = 32 * 1024
MAX_BLOBMETA_SERIALIZED_BYTES = 32 * 1024
MAX_OBJECT_BYTES = 4 * 1024 * 1024 * 1024
MAX_REJECTION_REASON_BYTES = 1024
DEFAULT_MAX_MRIDS = 32
DEFAULT_MAX_INTERVAL_HOURS = 24

METADATA_KEY_PATTERN = re.compile(r"[A-Za-z][A-Za-z0-9_.-]{0,63}\Z")
OBJECT_KEY_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,511}\Z")


class ContractValidationError(ValueError):
    """Raised when a session request or Blobmeta result violates its contract."""


def validate_measurement_session_request(
    request: MeasurementSessionRequest,
    max_mrids: int = DEFAULT_MAX_MRIDS,
    max_interval_hours: int = DEFAULT_MAX_INTERVAL_HOURS,
) -> None:
    """Validate one bounded raw-Protobuf request before historical extraction."""

    if max_mrids < 1:
        raise ContractValidationError("max_mrids must be at least one")
    if max_interval_hours < 1:
        raise ContractValidationError("max_interval_hours must be at least one")
    _canonical_uuid(request.session_id, "session_id")
    requested_at = _timestamp(request, "requested_at")
    started_at = _timestamp(request, "started_at")
    ended_at = _timestamp(request, "ended_at")
    del requested_at
    if started_at >= ended_at:
        raise ContractValidationError("started_at must be before ended_at")
    if ended_at - started_at > timedelta(hours=max_interval_hours):
        raise ContractValidationError("measurement interval exceeds configured maximum")
    _validate_mrids(request.mrids, max_mrids)
    _validate_metadata(request.metadata)
    if request.ByteSize() > MAX_REQUEST_SERIALIZED_BYTES:
        raise ContractValidationError(
            f"serialized request must not exceed {MAX_REQUEST_SERIALIZED_BYTES} bytes"
        )


def validate_blobmeta(result: Blobmeta) -> None:
    """Validate an immutable compacted output before publication or cataloging."""

    _canonical_uuid(result.session_id, "session_id")
    _bounded_text(result.blob_id, "blob_id", 512)
    _safe_object_key(result.blob_id, "blob_id")
    if len(result.request_sha256) != 32:
        raise ContractValidationError("request_sha256 must contain exactly 32 bytes")

    requested_at = _timestamp(result, "requested_at")
    started_at = _timestamp(result, "started_at")
    ended_at = _timestamp(result, "ended_at")
    finalized_at = _timestamp(result, "finalized_at")
    if started_at >= ended_at:
        raise ContractValidationError("started_at must be before ended_at")
    if finalized_at < requested_at:
        raise ContractValidationError("finalized_at must not precede requested_at")

    status = result.status
    if status not in {Blobmeta.COMPLETE, Blobmeta.PARTIAL, Blobmeta.REJECTED}:
        raise ContractValidationError("status must be COMPLETE, PARTIAL, or REJECTED")
    _validate_metadata(result.metadata)

    if status == Blobmeta.REJECTED:
        _validate_rejection(result)
    else:
        _validate_completed_result(result, status)

    if result.ByteSize() > MAX_BLOBMETA_SERIALIZED_BYTES:
        raise ContractValidationError(
            f"serialized Blobmeta must not exceed {MAX_BLOBMETA_SERIALIZED_BYTES} bytes"
        )


def request_sha256(request: MeasurementSessionRequest) -> bytes:
    """Return immutable evidence for a request's validated wire representation."""

    return sha256(request.SerializeToString(deterministic=True)).digest()


def successful_blob_id(session_id: str) -> str:
    """Return the one canonical blob identity for a materialized session."""

    _canonical_uuid(session_id, "session_id")
    return f"sessions/{session_id}/measurements"


def rejected_blob_id(session_id: str, request_digest: bytes) -> str:
    """Return a deterministic compacted identity for one rejected request."""

    _canonical_uuid(session_id, "session_id")
    if len(request_digest) != 32:
        raise ContractValidationError("request_digest must contain exactly 32 bytes")
    return f"rejections/{session_id}/{request_digest.hex()}"


def session_parquet_key(session_id: str) -> str:
    """Return the canonical immutable Parquet object key for one session."""

    _canonical_uuid(session_id, "session_id")
    return f"sessions/{session_id}/measurements.parquet"


def successful_receipt_key(session_id: str) -> str:
    """Return the immutable Blobmeta receipt key for a completed session."""

    _canonical_uuid(session_id, "session_id")
    return f"sessions/{session_id}/blobmeta.pb"


def rejected_receipt_key(session_id: str, request_digest: bytes) -> str:
    """Return the immutable Blobmeta receipt key for a rejected request."""

    return f"{rejected_blob_id(session_id, request_digest)}.pb"


def validate_kafka_key(key: bytes | None, expected: str, field_name: str) -> None:
    """Require a UTF-8 Kafka key to exactly match the Protobuf identity field."""

    if key != expected.encode("utf-8"):
        raise ContractValidationError(f"Kafka key does not match {field_name}")


def _validate_completed_result(result: Blobmeta, status: int) -> None:
    _validate_mrids(result.mrids, DEFAULT_MAX_MRIDS)
    if len(result.mrid_coverage) != len(result.mrids):
        raise ContractValidationError("mrid_coverage must contain exactly one entry for every MRID")
    coverage_total = 0
    missing_mrids = 0
    for mrid, coverage in zip(result.mrids, result.mrid_coverage, strict=True):
        if coverage.mrid != mrid:
            raise ContractValidationError("mrid_coverage entries must be ordered by requested MRID")
        coverage_total += coverage.measurement_count
        if coverage.measurement_count == 0:
            missing_mrids += 1
    if result.measurement_count != coverage_total:
        raise ContractValidationError("measurement_count must equal the sum of MRID coverage")
    if result.rejection_reason:
        raise ContractValidationError("completed Blobmeta must not contain rejection_reason")
    if not result.HasField("object"):
        raise ContractValidationError("completed Blobmeta requires an object reference")
    _validate_object_reference(result)
    if result.blob_id != successful_blob_id(result.session_id):
        raise ContractValidationError("completed Blobmeta uses an unexpected blob_id")
    if status == Blobmeta.COMPLETE and missing_mrids:
        raise ContractValidationError("COMPLETE Blobmeta cannot have missing MRIDs")
    if status == Blobmeta.PARTIAL and not missing_mrids:
        raise ContractValidationError("PARTIAL Blobmeta must identify at least one missing MRID")


def _validate_rejection(result: Blobmeta) -> None:
    if result.mrids or result.mrid_coverage or result.measurement_count:
        raise ContractValidationError("REJECTED Blobmeta must not contain measurement coverage")
    _bounded_text(result.rejection_reason, "rejection_reason", MAX_REJECTION_REASON_BYTES)
    if result.HasField("object"):
        raise ContractValidationError("REJECTED Blobmeta must not contain an object reference")
    expected_blob_id = rejected_blob_id(result.session_id, bytes(result.request_sha256))
    if result.blob_id != expected_blob_id:
        raise ContractValidationError("REJECTED Blobmeta uses an unexpected blob_id")


def _validate_object_reference(result: Blobmeta) -> None:
    reference = result.object
    if reference.bucket != DEFAULT_SESSION_BUCKET:
        raise ContractValidationError(f"object.bucket must be {DEFAULT_SESSION_BUCKET!r}")
    expected_key = session_parquet_key(result.session_id)
    if reference.object_key != expected_key:
        raise ContractValidationError("object.object_key must use the canonical session namespace")
    _safe_object_key(reference.object_key, "object.object_key")
    if reference.media_type != PARQUET_MEDIA_TYPE:
        raise ContractValidationError("object.media_type must be the canonical Parquet media type")
    if reference.parquet_schema_version != SESSION_PARQUET_SCHEMA_VERSION:
        raise ContractValidationError(
            f"object.parquet_schema_version must be {SESSION_PARQUET_SCHEMA_VERSION}"
        )
    if reference.byte_length == 0 or reference.byte_length > MAX_OBJECT_BYTES:
        raise ContractValidationError("object.byte_length is outside the supported range")
    if len(reference.sha256) != 32:
        raise ContractValidationError("object.sha256 must contain exactly 32 bytes")


def _validate_mrids(values: object, maximum: int) -> None:
    mrids = tuple(values)
    if not 1 <= len(mrids) <= maximum:
        raise ContractValidationError(f"mrids must contain between 1 and {maximum} entries")
    previous: str | None = None
    for mrid in mrids:
        _bounded_text(mrid, "mrid", MAX_MRID_BYTES)
        if previous is not None and mrid <= previous:
            raise ContractValidationError("mrids must be strictly sorted and unique")
        previous = mrid


def _validate_metadata(entries: object) -> None:
    metadata = tuple(entries)
    if len(metadata) > MAX_METADATA_ENTRIES:
        raise ContractValidationError(f"metadata must contain at most {MAX_METADATA_ENTRIES} entries")
    previous_key: str | None = None
    for entry in metadata:
        if not METADATA_KEY_PATTERN.fullmatch(entry.key):
            raise ContractValidationError(
                "metadata keys must use ASCII letters, digits, dots, underscores, or hyphens"
            )
        _bounded_text(entry.value, f"metadata.{entry.key}", MAX_METADATA_VALUE_BYTES)
        if previous_key is not None and entry.key <= previous_key:
            raise ContractValidationError("metadata entries must be strictly sorted by unique key")
        previous_key = entry.key


def _timestamp(message: object, field_name: str):
    if not message.HasField(field_name):
        raise ContractValidationError(f"{field_name} is required")
    timestamp = getattr(message, field_name)
    try:
        return timestamp.ToDatetime(tzinfo=timezone.utc)
    except ValueError as error:
        raise ContractValidationError(
            f"{field_name} is outside the Protobuf timestamp range"
        ) from error


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