"""Canonical compacted Alarm evaluation watermark encoding and validation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from google.protobuf.message import DecodeError
from google.protobuf.timestamp_pb2 import Timestamp

from processor_alarm_threshold.alarm import (
    AlarmContractError,
    AlarmIdentity,
    canonical_alarm_key,
    decode_alarm_key,
)
from processor_alarm_threshold.generated import alarm_pb2


@dataclass(frozen=True, order=True)
class EvaluationTime:
    """A validated protobuf timestamp without losing nanosecond precision."""

    seconds: int
    nanos: int

    @classmethod
    def from_datetime(cls, value: datetime) -> "EvaluationTime":
        """Convert a timezone-aware datetime without using a floating-point epoch."""

        if value.tzinfo is None:
            raise AlarmContractError("Alarm timestamp must include a timezone")
        utc_value = value.astimezone(timezone.utc)
        epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
        delta = utc_value - epoch
        return cls(
            seconds=delta.days * 86_400 + delta.seconds,
            nanos=delta.microseconds * 1_000,
        )

    @classmethod
    def from_timestamp(cls, value: Timestamp, description: str) -> "EvaluationTime":
        """Validate and retain an incoming protobuf Timestamp exactly."""

        try:
            value.ToDatetime(tzinfo=timezone.utc)
        except (OverflowError, ValueError) as error:
            raise AlarmContractError(f"{description} timestamp is invalid") from error
        return cls(seconds=value.seconds, nanos=value.nanos)

    def as_datetime(self) -> datetime:
        """Return the timestamp for freshness checks and Alarm evidence encoding."""

        return Timestamp(seconds=self.seconds, nanos=self.nanos).ToDatetime(tzinfo=timezone.utc)

    def copy_to(self, target: Timestamp) -> None:
        """Copy the exact seconds/nanos pair into a protobuf Timestamp field."""

        target.seconds = self.seconds
        target.nanos = self.nanos

    def timestamp_ms(self) -> int:
        """Return the Kafka record timestamp without changing the protobuf value."""

        return self.seconds * 1_000 + self.nanos // 1_000_000


@dataclass(frozen=True)
class WatermarkSnapshotEvent:
    """One configured-rule watermark upsert or compacted tombstone."""

    identity: AlarmIdentity
    last_evaluated_at: EvaluationTime | None


@dataclass(frozen=True)
class WatermarkOutput:
    """One raw-Protobuf watermark upsert for the compacted topic."""

    key: bytes
    timestamp_ms: int
    value: bytes


def decode_watermark_snapshot_event(
    key: bytes | None,
    value: bytes | None,
    configured_rule_ids: frozenset[str],
) -> WatermarkSnapshotEvent | None:
    """Decode a configured-rule compacted watermark record.

    Other producers' keys are ignored. A malformed record for one of this
    processor's rule IDs makes local recovery unsafe.
    """

    try:
        identity = decode_alarm_key(key)
    except AlarmContractError:
        return None
    if identity.rule_id not in configured_rule_ids:
        return None
    if value is None:
        return WatermarkSnapshotEvent(identity=identity, last_evaluated_at=None)
    message = alarm_pb2.AlarmEvaluationWatermark()
    try:
        message.ParseFromString(value)
    except DecodeError as error:
        raise AlarmContractError(
            "configured AlarmEvaluationWatermark value is not valid raw Protobuf"
        ) from error
    if (
        message.alarm_key != identity.alarm_key
        or message.rule_id != identity.rule_id
        or message.mrid != identity.mrid
    ):
        raise AlarmContractError(
            "configured AlarmEvaluationWatermark value does not match its Kafka key"
        )
    if not message.HasField("last_evaluated_at"):
        raise AlarmContractError("configured AlarmEvaluationWatermark lacks last_evaluated_at")
    return WatermarkSnapshotEvent(
        identity=identity,
        last_evaluated_at=EvaluationTime.from_timestamp(
            message.last_evaluated_at,
            "AlarmEvaluationWatermark last_evaluated_at",
        ),
    )


def watermark_output(
    rule_id: str,
    mrid: str,
    last_evaluated_at: EvaluationTime,
) -> WatermarkOutput:
    """Build an exact raw-Protobuf evaluation watermark upsert."""

    alarm_key = canonical_alarm_key(rule_id, mrid)
    message = alarm_pb2.AlarmEvaluationWatermark(
        alarm_key=alarm_key,
        rule_id=rule_id,
        mrid=mrid,
    )
    last_evaluated_at.copy_to(message.last_evaluated_at)
    return WatermarkOutput(
        key=alarm_key.encode("utf-8"),
        timestamp_ms=last_evaluated_at.timestamp_ms(),
        value=message.SerializeToString(),
    )