"""Serialized state transitions for threshold desired-state alarms."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import math
import uuid

from google.protobuf.message import DecodeError

from processor_alarm_threshold.alarm import (
    ActiveAlarm,
    AlarmContractError,
    AlarmOutput,
    AlarmSnapshotEvent,
    active_alarm_output,
    canonical_alarm_key,
    decode_snapshot_event,
    tombstone_output,
)
from processor_alarm_threshold.config import AlarmRule
from processor_alarm_threshold.generated import rtd_schema_pb2
from processor_alarm_threshold.masterdata import MasterdataRegistry
from processor_alarm_threshold.watermark import (
    EvaluationTime,
    WatermarkOutput,
    WatermarkSnapshotEvent,
    decode_watermark_snapshot_event,
    watermark_output,
)


_MAX_AGE = timedelta(seconds=30)
_EPISODE_NAMESPACE = uuid.UUID("f5d55f94-655f-5460-a315-f56a8a891007")


@dataclass(frozen=True)
class _Condition:
    active: ActiveAlarm | None
    watermark: EvaluationTime | None


@dataclass(frozen=True)
class _Observation:
    mrid: str
    observed_at: datetime
    evaluated_at: EvaluationTime
    value: float


@dataclass(frozen=True)
class StateOutputs:
    """Alarm transitions and their ordered evaluation watermark upserts."""

    alarm_outputs: tuple[AlarmOutput, ...] = ()
    watermark_outputs: tuple[WatermarkOutput, ...] = ()


class ThresholdState:
    """Own current threshold state without durable state outside compacted Kafka."""

    def __init__(
        self,
        catalog_id: str,
        rules: tuple[AlarmRule, ...],
        *,
        allow_legacy_active_alarm_bootstrap: bool = False,
    ) -> None:
        self._rules = rules
        self._rules_by_id = {rule.rule_id: rule for rule in rules}
        self._registry = MasterdataRegistry(catalog_id, rules)
        self._conditions: dict[str, _Condition] = {}
        self._alarm_snapshot: dict[str, AlarmSnapshotEvent] = {}
        self._watermark_snapshot: dict[str, WatermarkSnapshotEvent] = {}
        self._allow_legacy_active_alarm_bootstrap = allow_legacy_active_alarm_bootstrap

    @property
    def member_mrids(self) -> frozenset[str]:
        """Return currently eligible exact MRIDs, including the ready-idle empty set."""

        return self._registry.mrids

    @property
    def active_alarm_keys(self) -> frozenset[str]:
        """Return local configured-rule conditions currently reconstructed as active."""

        return frozenset(
            key for key, condition in self._conditions.items() if condition.active is not None
        )

    def fold_masterdata_snapshot(self, key: bytes | None, value: bytes | None) -> None:
        """Fold one bootstrap Masterdata record without emitting transient outputs."""

        self._registry.apply(key, value)

    def fold_alarm_snapshot(self, key: bytes | None, value: bytes | None) -> None:
        """Fold one bootstrap Alarm record or same-key tombstone."""

        event = decode_snapshot_event(key, value, frozenset(self._rules_by_id))
        if event is None:
            return
        self._alarm_snapshot[event.identity.alarm_key] = event

    def fold_watermark_snapshot(self, key: bytes | None, value: bytes | None) -> None:
        """Fold one bootstrap watermark record without treating tombstones as state."""

        event = decode_watermark_snapshot_event(key, value, frozenset(self._rules_by_id))
        if event is None:
            return
        if event.last_evaluated_at is None:
            self._watermark_snapshot.pop(event.identity.alarm_key, None)
            return
        self._watermark_snapshot[event.identity.alarm_key] = event

    def complete_initial_snapshot(self, timestamp_ms: int) -> StateOutputs:
        """Restore state from explicit snapshots and clear stale active desired state."""

        alarm_outputs: list[AlarmOutput] = []
        watermark_outputs: list[WatermarkOutput] = []
        snapshot_keys = set(self._alarm_snapshot).union(self._watermark_snapshot)
        for alarm_key in sorted(snapshot_keys):
            event = self._alarm_snapshot.get(alarm_key)
            watermark_event = self._watermark_snapshot.get(alarm_key)
            identity = event.identity if event is not None else watermark_event.identity
            rule = self._rules_by_id[identity.rule_id]
            watermark = (
                watermark_event.last_evaluated_at if watermark_event is not None else None
            )
            active = event.active if event is not None else None
            if active is not None:
                active_evidence = EvaluationTime.from_datetime(active.observed_at)
                if watermark is None:
                    if not self._allow_legacy_active_alarm_bootstrap:
                        raise AlarmContractError(
                            "configured active Alarm has no AlarmEvaluationWatermark; "
                            "set the explicit migration fence only for reviewed legacy recovery"
                        )
                    watermark = active_evidence
                    watermark_outputs.append(
                        watermark_output(rule.rule_id, identity.mrid, watermark)
                    )
                elif active_evidence > watermark:
                    watermark = active_evidence
                    watermark_outputs.append(
                        watermark_output(rule.rule_id, identity.mrid, watermark)
                    )
            if rule not in self._registry.rules_for(identity.mrid) and active is not None:
                alarm_outputs.append(tombstone_output(rule.rule_id, identity.mrid, timestamp_ms))
            self._conditions[alarm_key] = _Condition(
                active=active if rule in self._registry.rules_for(identity.mrid) else None,
                watermark=watermark,
            )
        self._alarm_snapshot.clear()
        self._watermark_snapshot.clear()
        return StateOutputs(
            alarm_outputs=tuple(alarm_outputs),
            watermark_outputs=tuple(watermark_outputs),
        )

    def apply_masterdata(
        self,
        key: bytes | None,
        value: bytes | None,
        timestamp_ms: int,
    ) -> StateOutputs:
        """Apply dynamic membership and clear active conditions without erasing watermarks."""

        change = self._registry.apply(key, value)
        alarm_outputs: list[AlarmOutput] = []
        for mrid in sorted(change.removed):
            for rule in self._rules:
                if not rule.selector.matches_mrid(mrid):
                    continue
                alarm_key = canonical_alarm_key(rule.rule_id, mrid)
                condition = self._conditions.get(alarm_key)
                if condition is not None and condition.active is not None:
                    alarm_outputs.append(tombstone_output(rule.rule_id, mrid, timestamp_ms))
                if condition is not None:
                    self._conditions[alarm_key] = _Condition(
                        active=None,
                        watermark=condition.watermark,
                    )
        return StateOutputs(alarm_outputs=tuple(alarm_outputs))

    def apply_alarm(self, key: bytes | None, value: bytes | None) -> None:
        """Apply a post-bootstrap configured-rule Alarm change without echoing it."""

        event = decode_snapshot_event(key, value, frozenset(self._rules_by_id))
        if event is None:
            return
        if self._rules_by_id[event.identity.rule_id] not in self._registry.rules_for(event.identity.mrid):
            return
        previous = self._conditions.get(event.identity.alarm_key)
        if event.active is None:
            self._conditions[event.identity.alarm_key] = _Condition(
                active=None,
                watermark=previous.watermark if previous else None,
            )
            return
        self._conditions[event.identity.alarm_key] = _Condition(
            active=event.active,
            watermark=previous.watermark if previous else None,
        )

    def apply_watermark(self, key: bytes | None, value: bytes | None) -> None:
        """Apply a post-bootstrap watermark without allowing a high-water regression."""

        event = decode_watermark_snapshot_event(key, value, frozenset(self._rules_by_id))
        if event is None or event.last_evaluated_at is None:
            return
        previous = self._conditions.get(event.identity.alarm_key)
        previous_watermark = previous.watermark if previous else None
        watermark = max(filter(None, (previous_watermark, event.last_evaluated_at)))
        self._conditions[event.identity.alarm_key] = _Condition(
            active=previous.active if previous else None,
            watermark=watermark,
        )

    def evaluate_live_measurement(
        self,
        *,
        key: bytes | None,
        value: bytes | None,
        topic: str,
        partition: int,
        offset: int,
        now: datetime,
    ) -> StateOutputs:
        """Evaluate one current measurement while leaving disqualified state unchanged."""

        observation = _qualifying_observation(key, value, now)
        if observation is None:
            return StateOutputs()
        rules = self._registry.rules_for(observation.mrid)
        if not rules:
            return StateOutputs()
        alarm_outputs: list[AlarmOutput] = []
        watermark_outputs: list[WatermarkOutput] = []
        for rule in rules:
            alarm_key = canonical_alarm_key(rule.rule_id, observation.mrid)
            previous = self._conditions.get(alarm_key)
            high_water = _condition_high_water(previous)
            if high_water is not None and observation.evaluated_at <= high_water:
                continue
            if observation.value > rule.threshold:
                active = previous.active if previous else None
                if active is None:
                    active = ActiveAlarm(
                        activated_at=observation.observed_at,
                        episode_id=_episode_id(alarm_key, topic, partition, offset),
                        observed_at=observation.observed_at,
                    )
                else:
                    active = ActiveAlarm(
                        activated_at=active.activated_at,
                        episode_id=active.episode_id,
                        observed_at=observation.observed_at,
                    )
                alarm_outputs.append(
                    active_alarm_output(
                        rule_id=rule.rule_id,
                        rule_revision=rule.revision,
                        mrid=observation.mrid,
                        severity=rule.severity,
                        episode_id=active.episode_id,
                        activated_at=active.activated_at,
                        observed_at=observation.observed_at,
                        threshold=rule.threshold,
                        value=observation.value,
                    )
                )
            elif previous and previous.active is not None:
                alarm_outputs.append(
                    tombstone_output(
                        rule.rule_id,
                        observation.mrid,
                        observation.evaluated_at.timestamp_ms(),
                    )
                )
            self._conditions[alarm_key] = _Condition(
                active=active if observation.value > rule.threshold else None,
                watermark=observation.evaluated_at,
            )
            watermark_outputs.append(
                watermark_output(rule.rule_id, observation.mrid, observation.evaluated_at)
            )
        return StateOutputs(
            alarm_outputs=tuple(alarm_outputs),
            watermark_outputs=tuple(watermark_outputs),
        )


def _condition_high_water(condition: _Condition | None) -> EvaluationTime | None:
    """Return the maximum durable watermark or current active Alarm evidence."""

    if condition is None:
        return None
    active_evidence = (
        EvaluationTime.from_datetime(condition.active.observed_at)
        if condition.active is not None
        else None
    )
    values = tuple(value for value in (condition.watermark, active_evidence) if value is not None)
    return max(values) if values else None


def _qualifying_observation(
    key: bytes | None,
    value: bytes | None,
    now: datetime,
) -> _Observation | None:
    if not isinstance(key, bytes) or value is None:
        return None
    message = rtd_schema_pb2.MCCSMeasurementValue()
    try:
        message.ParseFromString(value)
    except DecodeError:
        return None
    if key != message.mrid.encode("utf-8") or message.WhichOneof("value") != "double_value":
        return None
    if not math.isfinite(message.double_value) or not _has_qualifying_quality(message):
        return None
    if not message.HasField("timestamp_mccs"):
        return None
    try:
        evaluated_at = EvaluationTime.from_timestamp(
            message.timestamp_mccs,
            "LiveMeasurement timestamp_mccs",
        )
        observed_at = evaluated_at.as_datetime().astimezone(timezone.utc)
    except AlarmContractError:
        return None
    current_time = _utc(now)
    if current_time - observed_at > _MAX_AGE:
        return None
    return _Observation(
        mrid=message.mrid,
        observed_at=observed_at,
        evaluated_at=evaluated_at,
        value=message.double_value,
    )


def _has_qualifying_quality(message: rtd_schema_pb2.MCCSMeasurementValue) -> bool:
    if not message.HasField("quality") or not message.quality.HasField("valid"):
        return False
    if not message.quality.valid:
        return False
    return not any(
        message.quality.HasField(field) and getattr(message.quality, field)
        for field in ("substituted", "operator_blocked", "overflow", "old_data")
    )


def _episode_id(alarm_key: str, topic: str, partition: int, offset: int) -> str:
    name = json.dumps(
        [alarm_key, topic, partition, offset],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    return str(uuid.uuid5(_EPISODE_NAMESPACE, name))


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("Current time must include a timezone")
    return value.astimezone(timezone.utc)