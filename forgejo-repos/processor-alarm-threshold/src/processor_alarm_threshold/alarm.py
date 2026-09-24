"""Canonical compacted Alarm record encoding and snapshot validation."""

from __future__ import annotations

import base64
from dataclasses import dataclass
from datetime import datetime, timezone
import re
import uuid

from google.protobuf.message import DecodeError

from processor_alarm_threshold.generated import alarm_pb2


class AlarmContractError(ValueError):
    """Raised when a configured rule's Alarm record violates the root contract."""


_BASE64URL = re.compile(r"[A-Za-z0-9_-]+\Z")


@dataclass(frozen=True)
class AlarmIdentity:
    """The exact stable identity encoded by an Alarm Kafka key."""

    alarm_key: str
    mrid: str
    rule_id: str


@dataclass(frozen=True)
class ActiveAlarm:
    """The replayed durable state needed to preserve an alarm episode."""

    activated_at: datetime
    episode_id: str
    observed_at: datetime


@dataclass(frozen=True)
class AlarmSnapshotEvent:
    """One configured-rule Alarm upsert or same-key tombstone."""

    active: ActiveAlarm | None
    identity: AlarmIdentity


@dataclass(frozen=True)
class AlarmOutput:
    """One acknowledged raw-Protobuf Alarm upsert or compacted tombstone."""

    key: bytes
    timestamp_ms: int
    value: bytes | None


def canonical_alarm_key(rule_id: str, mrid: str) -> str:
    """Encode a collision-free root-contract Alarm identity."""

    if not rule_id or not mrid:
        raise AlarmContractError("Alarm rule_id and mrid must be non-empty")
    return f"alarm/v1/{_base64url(rule_id)}/{_base64url(mrid)}"


def decode_alarm_key(key: bytes | None) -> AlarmIdentity:
    """Decode and canonicality-check the root Alarm key encoding."""

    if not isinstance(key, bytes) or not key:
        raise AlarmContractError("Alarm record has no UTF-8 key")
    try:
        text = key.decode("utf-8")
    except UnicodeDecodeError as error:
        raise AlarmContractError("Alarm key is not UTF-8") from error
    parts = text.split("/")
    if len(parts) != 4 or parts[:2] != ["alarm", "v1"]:
        raise AlarmContractError("Alarm key does not use alarm/v1 encoding")
    rule_id = _decode_base64url(parts[2], "rule_id")
    mrid = _decode_base64url(parts[3], "mrid")
    if canonical_alarm_key(rule_id, mrid) != text:
        raise AlarmContractError("Alarm key is not canonical")
    return AlarmIdentity(alarm_key=text, rule_id=rule_id, mrid=mrid)


def decode_snapshot_event(
    key: bytes | None,
    value: bytes | None,
    configured_rule_ids: frozenset[str],
) -> AlarmSnapshotEvent | None:
    """Decode one configured rule's compacted desired-state record.

    Other producers' keys are intentionally ignored because `Alarm` is a
    shared root-owned topic. A malformed record that claims one of this
    processor's rule IDs makes the local snapshot unsafe.
    """

    try:
        identity = decode_alarm_key(key)
    except AlarmContractError:
        return None
    if identity.rule_id not in configured_rule_ids:
        return None
    if value is None:
        return AlarmSnapshotEvent(active=None, identity=identity)
    message = alarm_pb2.AlarmDesiredState()
    try:
        message.ParseFromString(value)
    except DecodeError as error:
        raise AlarmContractError("configured Alarm value is not valid raw Protobuf") from error
    if (
        message.alarm_key != identity.alarm_key
        or message.rule_id != identity.rule_id
        or message.mrid != identity.mrid
    ):
        raise AlarmContractError("configured Alarm value does not match its Kafka key")
    if message.severity not in {
        alarm_pb2.ALARM_SEVERITY_WARNING,
        alarm_pb2.ALARM_SEVERITY_CRITICAL,
    }:
        raise AlarmContractError("configured Alarm value has an unsupported severity")
    if not message.episode_id or not message.rule_revision:
        raise AlarmContractError("configured Alarm value lacks episode or rule revision")
    _validate_canonical_uuid(message.episode_id)
    activated_at = _timestamp(message, "activated_at", "Alarm activation")
    if not message.HasField("current_evidence"):
        raise AlarmContractError("configured Alarm value lacks current evidence")
    observed_at = _timestamp(
        message.current_evidence,
        "observed_at",
        "Alarm current evidence",
    )
    return AlarmSnapshotEvent(
        active=ActiveAlarm(
            activated_at=activated_at,
            episode_id=message.episode_id,
            observed_at=observed_at,
        ),
        identity=identity,
    )


def active_alarm_output(
    *,
    rule_id: str,
    rule_revision: str,
    mrid: str,
    severity: str,
    episode_id: str,
    activated_at: datetime,
    observed_at: datetime,
    threshold: float,
    value: float,
) -> AlarmOutput:
    """Build the current desired active state for one evaluated threshold."""

    identity = AlarmIdentity(
        alarm_key=canonical_alarm_key(rule_id, mrid),
        rule_id=rule_id,
        mrid=mrid,
    )
    message = alarm_pb2.AlarmDesiredState(
        alarm_key=identity.alarm_key,
        episode_id=episode_id,
        rule_id=rule_id,
        mrid=mrid,
        severity=_severity_value(severity),
        rule_revision=rule_revision,
    )
    message.activated_at.FromDatetime(_utc(activated_at))
    message.current_evidence.observed_at.FromDatetime(_utc(observed_at))
    message.current_evidence.summary = (
        f"Frequency {_number(value)} Hz is greater than threshold {_number(threshold)} Hz"
    )
    for name, attribute_value in (
        ("operator", "gt"),
        ("threshold", _number(threshold)),
        ("value", _number(value)),
        ("unit", "Hz"),
    ):
        attribute = message.current_evidence.attributes.add()
        attribute.name = name
        attribute.value = attribute_value
    return AlarmOutput(
        key=identity.alarm_key.encode("utf-8"),
        timestamp_ms=int(_utc(observed_at).timestamp() * 1_000),
        value=message.SerializeToString(),
    )


def tombstone_output(rule_id: str, mrid: str, timestamp_ms: int) -> AlarmOutput:
    """Build a same-key compacted tombstone for one active condition."""

    return AlarmOutput(
        key=canonical_alarm_key(rule_id, mrid).encode("utf-8"),
        timestamp_ms=timestamp_ms,
        value=None,
    )


def _base64url(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode("utf-8")).rstrip(b"=").decode("ascii")


def _decode_base64url(value: str, field: str) -> str:
    if not _BASE64URL.fullmatch(value) or len(value) % 4 == 1:
        raise AlarmContractError(f"Alarm key has an invalid {field} component")
    try:
        decoded = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
        text = decoded.decode("utf-8")
    except (UnicodeDecodeError, ValueError) as error:
        raise AlarmContractError(f"Alarm key has a non-UTF-8 {field} component") from error
    if not text:
        raise AlarmContractError(f"Alarm key has an empty {field} component")
    return text


def _timestamp(message: object, field: str, description: str) -> datetime:
    if not message.HasField(field):
        raise AlarmContractError(f"{description} timestamp is missing")
    try:
        return _utc(getattr(message, field).ToDatetime(tzinfo=timezone.utc))
    except ValueError as error:
        raise AlarmContractError(f"{description} timestamp is invalid") from error


def _severity_value(value: str) -> int:
    if value == "warning":
        return alarm_pb2.ALARM_SEVERITY_WARNING
    if value == "critical":
        return alarm_pb2.ALARM_SEVERITY_CRITICAL
    raise AlarmContractError(f"Unsupported alarm severity {value!r}")


def _validate_canonical_uuid(value: str) -> None:
    try:
        parsed = uuid.UUID(value)
    except ValueError as error:
        raise AlarmContractError("Alarm episode_id is not a UUID") from error
    if str(parsed) != value:
        raise AlarmContractError("Alarm episode_id is not a canonical UUID")


def _number(value: float) -> str:
    return format(value, ".15g")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise AlarmContractError("Alarm timestamp must include a timezone")
    return value.astimezone(timezone.utc)