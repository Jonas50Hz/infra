"""Stateful, event-time frequency capture into bounded session requests."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
import json
import logging
import math
import time
from uuid import UUID, uuid5

from google.protobuf.timestamp_pb2 import Timestamp

from processor_frequency_measurement_session.generated import (
    measurement_session_pb2,
    rtd_schema_pb2,
)
from processor_frequency_measurement_session.policy import (
    CAPTURE_REASON,
    FREQUENCY_THRESHOLD_HZ,
    MAX_SESSION_DURATION,
    PROCESSING_DELAY_SECONDS,
    REQUEST_ORIGIN,
    WINDOW_PADDING,
    FrequencyCapturePolicy,
    policy_for,
)


LOGGER = logging.getLogger(__name__)
_SESSION_NAMESPACE = UUID("8f3c89c2-0b2f-5a40-b6ed-7e22a1b24f63")
_MIN_TIMESTAMP_SECONDS = -62_135_596_800
_MAX_TIMESTAMP_SECONDS = 253_402_300_799
_NANOSECONDS_PER_SECOND = 1_000_000_000
_MILLISECONDS_PER_SECOND = 1_000
_MAX_SESSION_MRIDS = 32
_DISQUALIFYING_QUALITY_FIELDS = (
    "substituted",
    "operator_blocked",
    "overflow",
    "old_data",
)


@dataclass(frozen=True, order=True)
class EventTime:
    """A lossless, ordered Protobuf timestamp suitable for session event time."""

    seconds: int
    nanos: int

    def __post_init__(self) -> None:
        if not _MIN_TIMESTAMP_SECONDS <= self.seconds <= _MAX_TIMESTAMP_SECONDS:
            raise ValueError("timestamp seconds are outside the Protobuf Timestamp range")
        if not 0 <= self.nanos < _NANOSECONDS_PER_SECOND:
            raise ValueError("timestamp nanos are outside the Protobuf Timestamp range")

    @classmethod
    def from_protobuf(cls, value: Timestamp) -> "EventTime":
        """Copy and validate one present Protobuf timestamp without losing nanos."""

        return cls(seconds=value.seconds, nanos=value.nanos)

    def plus(self, interval: timedelta) -> "EventTime":
        """Return a lossless timestamp shifted by the supplied interval."""

        interval_nanoseconds = (
            (interval.days * 86_400 + interval.seconds) * _NANOSECONDS_PER_SECOND
            + interval.microseconds * 1_000
        )
        seconds, nanos = divmod(
            self.seconds * _NANOSECONDS_PER_SECOND + self.nanos + interval_nanoseconds,
            _NANOSECONDS_PER_SECOND,
        )
        return EventTime(seconds=seconds, nanos=nanos)

    def milliseconds(self) -> int:
        """Return the event timestamp rounded down to Kafka millisecond precision."""

        return self.seconds * _MILLISECONDS_PER_SECOND + self.nanos // 1_000_000

    def copy_to(self, target: Timestamp) -> None:
        """Set an output Protobuf timestamp without datetime precision loss."""

        target.seconds = self.seconds
        target.nanos = self.nanos

    def rfc3339(self) -> str:
        """Return the canonical timestamp text used in a structured log record."""

        return Timestamp(seconds=self.seconds, nanos=self.nanos).ToJsonString()


@dataclass(frozen=True)
class QualifiedFrequency:
    """One explicitly configured, finite, timely frequency observation."""

    policy: FrequencyCapturePolicy
    event_time: EventTime
    value: float


@dataclass(frozen=True)
class SessionRequestEnvelope:
    """One request paired with its canonical output Kafka timestamp."""

    request: measurement_session_pb2.MeasurementSessionRequest
    kafka_timestamp_ms: int


@dataclass
class CaptureMetrics:
    """Small observable counters for normal PoC process inspection and tests."""

    over_limit_dropped_total: int = 0


@dataclass(frozen=True)
class _CaptureEpisode:
    """The in-memory onset retained until the first qualifying clearance."""

    policy: FrequencyCapturePolicy
    onset: EventTime


class EpisodeTracker:
    """Track independent per-frequency episodes in process-local memory only."""

    def __init__(
        self,
        *,
        sleeper: Callable[[float], None] = time.sleep,
        metrics: CaptureMetrics | None = None,
    ) -> None:
        self._sleeper = sleeper
        self._metrics = CaptureMetrics() if metrics is None else metrics
        self._episodes: dict[str, _CaptureEpisode] = {}
        self._last_event_times: dict[str, EventTime] = {}

    @property
    def metrics(self) -> CaptureMetrics:
        """Return counters accumulated by this in-memory tracker instance."""

        return self._metrics

    def transform(
        self,
        source: rtd_schema_pb2.MCCSMeasurementValue,
        key: bytes | None,
    ) -> SessionRequestEnvelope | None:
        """Advance one qualified event-time episode and emit only on clearance."""

        observation = qualify_frequency(source, key)
        if observation is None:
            return None

        frequency_mrid = observation.policy.frequency_mrid
        previous_event_time = self._last_event_times.get(frequency_mrid)
        if previous_event_time is not None and observation.event_time <= previous_event_time:
            return None
        self._last_event_times[frequency_mrid] = observation.event_time

        episode = self._episodes.get(frequency_mrid)
        if episode is None:
            if observation.value > FREQUENCY_THRESHOLD_HZ:
                self._episodes[frequency_mrid] = _CaptureEpisode(
                    policy=observation.policy,
                    onset=observation.event_time,
                )
            return None

        if observation.value > FREQUENCY_THRESHOLD_HZ:
            return None

        del self._episodes[frequency_mrid]
        return self._close_episode(episode, observation.event_time)

    def _close_episode(
        self,
        episode: _CaptureEpisode,
        clearance: EventTime,
    ) -> SessionRequestEnvelope | None:
        started_at = episode.onset.plus(-WINDOW_PADDING)
        ended_at = clearance.plus(WINDOW_PADDING)
        padded_duration_nanoseconds = _duration_nanoseconds(started_at, ended_at)
        maximum_duration_nanoseconds = _timedelta_nanoseconds(MAX_SESSION_DURATION)
        if padded_duration_nanoseconds > maximum_duration_nanoseconds:
            self._metrics.over_limit_dropped_total += 1
            LOGGER.error(
                json.dumps(
                    {
                        "clearance_timestamp_mccs": clearance.rfc3339(),
                        "event": "measurement_session_over_limit_dropped",
                        "frequency_mrid": episode.policy.frequency_mrid,
                        "max_duration_seconds": int(MAX_SESSION_DURATION.total_seconds()),
                        "onset_timestamp_mccs": episode.onset.rfc3339(),
                        "padded_duration_nanoseconds": padded_duration_nanoseconds,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                )
            )
            return None

        request = build_session_request(episode.policy, episode.onset, clearance)
        self._sleeper(PROCESSING_DELAY_SECONDS)
        return SessionRequestEnvelope(
            request=request,
            kafka_timestamp_ms=clearance.milliseconds(),
        )


def qualify_frequency(
    source: rtd_schema_pb2.MCCSMeasurementValue,
    key: bytes | None,
) -> QualifiedFrequency | None:
    """Accept only one explicit, quality-qualified, finite, timestamped frequency."""

    policy = policy_for(source.mrid)
    if policy is None or key != source.mrid.encode("utf-8"):
        return None
    if source.WhichOneof("value") != "double_value" or not math.isfinite(source.double_value):
        return None
    if not _has_qualifying_quality(source) or not source.HasField("timestamp_mccs"):
        return None
    try:
        event_time = EventTime.from_protobuf(source.timestamp_mccs)
    except ValueError:
        return None
    return QualifiedFrequency(
        policy=policy,
        event_time=event_time,
        value=source.double_value,
    )


def build_session_request(
    policy: FrequencyCapturePolicy,
    onset: EventTime,
    clearance: EventTime,
) -> measurement_session_pb2.MeasurementSessionRequest:
    """Build the canonical deterministic request for one closed capture episode."""

    started_at = onset.plus(-WINDOW_PADDING)
    ended_at = clearance.plus(WINDOW_PADDING)
    request = measurement_session_pb2.MeasurementSessionRequest(
        session_id=_session_id(policy, onset, clearance),
        mrids=policy.capture_mrids,
    )
    clearance.copy_to(request.requested_at)
    started_at.copy_to(request.started_at)
    ended_at.copy_to(request.ended_at)
    for key, value in sorted(
        {
            "capture_reason": CAPTURE_REASON,
            "request_origin": REQUEST_ORIGIN,
        }.items()
    ):
        entry = request.metadata.add()
        entry.key = key
        entry.value = value
    _validate_session_request(request)
    return request


def request_key(request: measurement_session_pb2.MeasurementSessionRequest) -> bytes:
    """Return the canonical Kafka key for one immutable session request."""

    return request.session_id.encode("utf-8")


def _has_qualifying_quality(source: rtd_schema_pb2.MCCSMeasurementValue) -> bool:
    if not source.HasField("quality") or not source.quality.HasField("valid"):
        return False
    if not source.quality.valid:
        return False
    return not any(
        source.quality.HasField(field) and getattr(source.quality, field)
        for field in _DISQUALIFYING_QUALITY_FIELDS
    )


def _session_id(
    policy: FrequencyCapturePolicy,
    onset: EventTime,
    clearance: EventTime,
) -> str:
    name = json.dumps(
        {
            "capture_mrids": policy.capture_mrids,
            "capture_reason": CAPTURE_REASON,
            "clearance": [clearance.seconds, clearance.nanos],
            "frequency_mrid": policy.frequency_mrid,
            "onset": [onset.seconds, onset.nanos],
            "threshold_hz": FREQUENCY_THRESHOLD_HZ,
        },
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    )
    return str(uuid5(_SESSION_NAMESPACE, name))


def _validate_session_request(request: measurement_session_pb2.MeasurementSessionRequest) -> None:
    try:
        canonical_session_id = str(UUID(request.session_id))
    except ValueError as error:
        raise ValueError("session_id must be a UUID") from error
    if request.session_id != canonical_session_id:
        raise ValueError("session_id must use canonical UUID text")
    if not all(
        request.HasField(field)
        for field in ("requested_at", "started_at", "ended_at")
    ):
        raise ValueError("session request timestamps must be present")

    requested_at = EventTime.from_protobuf(request.requested_at)
    started_at = EventTime.from_protobuf(request.started_at)
    ended_at = EventTime.from_protobuf(request.ended_at)
    del requested_at
    if started_at >= ended_at:
        raise ValueError("session request must have a positive interval")
    if _duration_nanoseconds(started_at, ended_at) > _timedelta_nanoseconds(MAX_SESSION_DURATION):
        raise ValueError("session request interval exceeds 24 hours")

    mrids = tuple(request.mrids)
    if not mrids or len(mrids) > _MAX_SESSION_MRIDS:
        raise ValueError("session request MRID count is outside the contract limit")
    if mrids != tuple(sorted(set(mrids))):
        raise ValueError("session request MRIDs must be sorted and unique")

    metadata = tuple((entry.key, entry.value) for entry in request.metadata)
    if metadata != tuple(sorted(metadata)) or len({key for key, _value in metadata}) != len(metadata):
        raise ValueError("session request metadata must be sorted with unique keys")


def _duration_nanoseconds(started_at: EventTime, ended_at: EventTime) -> int:
    return (
        (ended_at.seconds - started_at.seconds) * _NANOSECONDS_PER_SECOND
        + ended_at.nanos
        - started_at.nanos
    )


def _timedelta_nanoseconds(value: timedelta) -> int:
    return (
        (value.days * 86_400 + value.seconds) * _NANOSECONDS_PER_SECOND
        + value.microseconds * 1_000
    )