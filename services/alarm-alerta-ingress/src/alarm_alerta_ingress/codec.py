"""Strict decoding for raw-Protobuf compacted Alarm records."""

from __future__ import annotations

import base64
from datetime import datetime, timezone
import re
import uuid

from google.protobuf.message import DecodeError

from alarm_alerta_ingress.generated import alarm_pb2
from alarm_alerta_ingress.model import AlarmIdentity, DesiredAlarm


_BASE64URL = re.compile(r"[A-Za-z0-9_-]+\Z")
_SEVERITIES = {
    alarm_pb2.ALARM_SEVERITY_WARNING: "WARNING",
    alarm_pb2.ALARM_SEVERITY_CRITICAL: "CRITICAL",
}


class AlarmDecodeError(ValueError):
    """Raised when an Alarm key or value violates the canonical contract."""


def decode_alarm_key(key: bytes | None) -> AlarmIdentity:
    """Decode and canonicality-check one compacted Alarm key."""

    if not isinstance(key, bytes) or not key:
        raise AlarmDecodeError("Alarm record has no UTF-8 key")
    try:
        alarm_key = key.decode("utf-8")
    except UnicodeDecodeError as error:
        raise AlarmDecodeError("Alarm Kafka key is not UTF-8") from error
    parts = alarm_key.split("/")
    if len(parts) != 4 or parts[:2] != ["alarm", "v1"]:
        raise AlarmDecodeError("Alarm key does not use alarm/v1 encoding")
    rule_id = _decode_base64url(parts[2], "rule_id")
    mrid = _decode_base64url(parts[3], "mrid")
    if canonical_alarm_key(rule_id, mrid) != alarm_key:
        raise AlarmDecodeError("Alarm key is not canonical")
    return AlarmIdentity(alarm_key=alarm_key, mrid=mrid, rule_id=rule_id)


def decode_alarm(key: bytes | None, value: bytes | None) -> DesiredAlarm:
    """Decode one non-null Alarm value and validate its key identity."""

    identity = decode_alarm_key(key)
    if value is None:
        raise AlarmDecodeError("Active Alarm record has no payload")
    message = alarm_pb2.AlarmDesiredState()
    try:
        message.ParseFromString(value)
    except DecodeError as error:
        raise AlarmDecodeError("Alarm value is not valid raw Protobuf") from error
    if (
        message.alarm_key != identity.alarm_key
        or message.rule_id != identity.rule_id
        or message.mrid != identity.mrid
    ):
        raise AlarmDecodeError("Alarm value identity does not match its Kafka key")
    severity = _SEVERITIES.get(message.severity)
    if severity is None:
        raise AlarmDecodeError("Alarm value has an unsupported severity")
    _validate_canonical_uuid(message.episode_id)
    if not message.rule_revision:
        raise AlarmDecodeError("Alarm value has no rule revision")
    activated_at = _timestamp(message, "activated_at", "Alarm activation")
    if not message.HasField("current_evidence"):
        raise AlarmDecodeError("Alarm value has no current evidence")
    evidence = message.current_evidence
    observed_at = _timestamp(evidence, "observed_at", "Alarm evidence")
    if not evidence.summary:
        raise AlarmDecodeError("Alarm evidence has no summary")
    evidence_attributes = _evidence_attributes(evidence)
    return DesiredAlarm(
        activated_at=activated_at,
        episode_id=message.episode_id,
        evidence=evidence_attributes,
        identity=identity,
        observed_at=observed_at,
        rule_revision=message.rule_revision,
        severity=severity,
        summary=evidence.summary,
    )


def canonical_alarm_key(rule_id: str, mrid: str) -> str:
    """Encode the unambiguous root-contract Alarm key."""

    if not rule_id or not mrid:
        raise AlarmDecodeError("Alarm rule_id and mrid must be non-empty")
    return f"alarm/v1/{_base64url(rule_id)}/{_base64url(mrid)}"


def _base64url(value: str) -> str:
    return base64.urlsafe_b64encode(value.encode("utf-8")).rstrip(b"=").decode("ascii")


def _decode_base64url(value: str, field: str) -> str:
    if not _BASE64URL.fullmatch(value) or len(value) % 4 == 1:
        raise AlarmDecodeError(f"Alarm key has an invalid {field} component")
    try:
        decoded = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
        text = decoded.decode("utf-8")
    except (UnicodeDecodeError, ValueError) as error:
        raise AlarmDecodeError(f"Alarm key has a non-UTF-8 {field} component") from error
    if not text:
        raise AlarmDecodeError(f"Alarm key has an empty {field} component")
    return text


def _evidence_attributes(evidence: object) -> tuple[tuple[str, str], ...]:
    attributes: list[tuple[str, str]] = []
    names: set[str] = set()
    for attribute in evidence.attributes:
        if not attribute.name or not attribute.value:
            raise AlarmDecodeError("Alarm evidence has an empty attribute")
        if attribute.name in names:
            raise AlarmDecodeError("Alarm evidence repeats an attribute name")
        names.add(attribute.name)
        attributes.append((attribute.name, attribute.value))
    return tuple(sorted(attributes))


def _timestamp(message: object, field: str, description: str) -> datetime:
    if not message.HasField(field):
        raise AlarmDecodeError(f"{description} timestamp is missing")
    try:
        return getattr(message, field).ToDatetime(tzinfo=timezone.utc)
    except ValueError as error:
        raise AlarmDecodeError(f"{description} timestamp is invalid") from error


def _validate_canonical_uuid(value: str) -> None:
    try:
        parsed = uuid.UUID(value)
    except ValueError as error:
        raise AlarmDecodeError("Alarm episode_id is not a UUID") from error
    if str(parsed) != value:
        raise AlarmDecodeError("Alarm episode_id is not a canonical UUID")