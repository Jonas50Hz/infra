"""Event-time episode and canonical session-request tests."""

from __future__ import annotations

import math
import unittest

from processor_frequency_measurement_session.capture import (
    CaptureMetrics,
    EpisodeTracker,
    qualify_frequency,
    request_key,
)
from processor_frequency_measurement_session.generated import rtd_schema_pb2
from processor_frequency_measurement_session.policy import CAPTURE_POLICIES


FREQUENCY_MRID = CAPTURE_POLICIES[0].frequency_mrid


class CaptureTests(unittest.TestCase):
    """Require explicit qualification and independent bounded capture episodes."""

    def test_qualifies_only_explicit_finite_non_disqualifying_frequency(self) -> None:
        source = _frequency(50.3, seconds=1_726_000_100, nanos=123_456_789)

        qualified = qualify_frequency(source, source.mrid.encode("utf-8"))

        self.assertIsNotNone(qualified)
        assert qualified is not None
        self.assertEqual(qualified.event_time.seconds, 1_726_000_100)
        self.assertEqual(qualified.event_time.nanos, 123_456_789)

        self.assertIsNone(qualify_frequency(source, b"wrong"))
        source.mrid = "urn:wama:poc:pmu:bay-06:frequency"
        self.assertIsNone(qualify_frequency(source, source.mrid.encode("utf-8")))
        source = _frequency(math.nan)
        self.assertIsNone(qualify_frequency(source, source.mrid.encode("utf-8")))
        source = _frequency(50.3)
        source.quality.valid = False
        self.assertIsNone(qualify_frequency(source, source.mrid.encode("utf-8")))
        source = _frequency(50.3)
        source.quality.substituted = True
        self.assertIsNone(qualify_frequency(source, source.mrid.encode("utf-8")))
        source = rtd_schema_pb2.MCCSMeasurementValue(mrid=FREQUENCY_MRID, double_value=50.3)
        source.quality.valid = True
        self.assertIsNone(qualify_frequency(source, source.mrid.encode("utf-8")))

    def test_rejected_inputs_do_not_open_or_close_an_episode(self) -> None:
        rejected_sources = (
            ("infinity", lambda seconds: _frequency(math.inf, seconds=seconds)),
            ("integer scalar", lambda seconds: _integer_frequency(seconds=seconds)),
            ("absent quality.valid", lambda seconds: _without_quality_valid(seconds=seconds)),
            (
                "operator_blocked",
                lambda seconds: _frequency_with_quality_flag(
                    "operator_blocked", seconds=seconds
                ),
            ),
            (
                "overflow",
                lambda seconds: _frequency_with_quality_flag("overflow", seconds=seconds),
            ),
            (
                "old_data",
                lambda seconds: _frequency_with_quality_flag("old_data", seconds=seconds),
            ),
        )

        for description, rejected_source in rejected_sources:
            with self.subTest(description=description):
                tracker = EpisodeTracker(sleeper=lambda _seconds: None)

                self.assertIsNone(
                    tracker.transform(rejected_source(100), FREQUENCY_MRID.encode())
                )
                self.assertIsNone(
                    tracker.transform(_frequency(50.0, seconds=101), FREQUENCY_MRID.encode())
                )
                self.assertIsNone(
                    tracker.transform(_frequency(50.3, seconds=102), FREQUENCY_MRID.encode())
                )
                self.assertIsNone(
                    tracker.transform(rejected_source(103), FREQUENCY_MRID.encode())
                )
                self.assertIsNotNone(
                    tracker.transform(_frequency(50.0, seconds=104), FREQUENCY_MRID.encode())
                )

    def test_starts_strictly_above_and_closes_at_the_exact_threshold(self) -> None:
        waits: list[float] = []
        tracker = EpisodeTracker(sleeper=waits.append)

        self.assertIsNone(tracker.transform(_frequency(50.2, seconds=100), FREQUENCY_MRID.encode()))
        self.assertIsNone(tracker.transform(_frequency(50.2001, seconds=101), FREQUENCY_MRID.encode()))
        envelope = tracker.transform(_frequency(50.2, seconds=102), FREQUENCY_MRID.encode())

        self.assertIsNotNone(envelope)
        assert envelope is not None
        self.assertEqual(envelope.request.started_at.seconds, 91)
        self.assertEqual(envelope.request.ended_at.seconds, 112)
        self.assertEqual(envelope.request.requested_at.seconds, 102)
        self.assertEqual(waits, [10.0])

    def test_ignores_non_newer_timestamp_mccs(self) -> None:
        tracker = EpisodeTracker(sleeper=lambda _seconds: None)

        self.assertIsNone(tracker.transform(_frequency(50.3, seconds=100), FREQUENCY_MRID.encode()))
        self.assertIsNone(tracker.transform(_frequency(50.0, seconds=100), FREQUENCY_MRID.encode()))

        envelope = tracker.transform(_frequency(50.0, seconds=101), FREQUENCY_MRID.encode())

        self.assertIsNotNone(envelope)

    def test_over_limit_episode_is_dropped_with_an_observable_counter(self) -> None:
        metrics = CaptureMetrics()
        tracker = EpisodeTracker(sleeper=lambda _seconds: None, metrics=metrics)

        self.assertIsNone(tracker.transform(_frequency(50.3, seconds=100), FREQUENCY_MRID.encode()))
        with self.assertLogs("processor_frequency_measurement_session.capture", level="ERROR") as logged:
            envelope = tracker.transform(
                _frequency(50.0, seconds=100 + 24 * 60 * 60),
                FREQUENCY_MRID.encode(),
            )

        self.assertIsNone(envelope)
        self.assertEqual(metrics.over_limit_dropped_total, 1)
        self.assertIn("measurement_session_over_limit_dropped", logged.output[0])

    def test_reentry_after_the_delay_is_a_new_episode(self) -> None:
        waits: list[float] = []
        tracker = EpisodeTracker(sleeper=waits.append)

        self.assertIsNone(tracker.transform(_frequency(50.3, seconds=100), FREQUENCY_MRID.encode()))
        first = tracker.transform(_frequency(50.0, seconds=101), FREQUENCY_MRID.encode())
        self.assertIsNotNone(first)
        self.assertIsNone(tracker.transform(_frequency(50.3, seconds=102), FREQUENCY_MRID.encode()))
        second = tracker.transform(_frequency(50.0, seconds=103), FREQUENCY_MRID.encode())

        self.assertIsNotNone(second)
        assert first is not None
        assert second is not None
        self.assertNotEqual(first.request.session_id, second.request.session_id)
        self.assertEqual(waits, [10.0, 10.0])

    def test_restart_discards_an_open_episode_without_emitting_a_truncated_session(self) -> None:
        first_tracker = EpisodeTracker(sleeper=lambda _seconds: None)

        self.assertIsNone(first_tracker.transform(_frequency(50.3, seconds=100), FREQUENCY_MRID.encode()))

        restarted_tracker = EpisodeTracker(sleeper=lambda _seconds: None)
        self.assertIsNone(restarted_tracker.transform(_frequency(50.0, seconds=101), FREQUENCY_MRID.encode()))

    def test_request_key_payload_and_timestamp_are_deterministic(self) -> None:
        first = _closed_envelope()
        second = _closed_envelope()

        self.assertEqual(first.request.session_id, second.request.session_id)
        self.assertEqual(
            first.request.SerializeToString(deterministic=True),
            second.request.SerializeToString(deterministic=True),
        )
        self.assertEqual(request_key(first.request), first.request.session_id.encode("utf-8"))
        self.assertEqual(first.kafka_timestamp_ms, 1_726_000_101_987)
        self.assertEqual(first.request.requested_at.nanos, 987_654_321)
        self.assertEqual(tuple(first.request.mrids), tuple(sorted(first.request.mrids)))
        self.assertEqual(
            tuple((entry.key, entry.value) for entry in first.request.metadata),
            (("capture_reason", "frequency_gt_50_2_hz"), ("request_origin", "processor-frequency-measurement-session")),
        )


def _closed_envelope():
    tracker = EpisodeTracker(sleeper=lambda _seconds: None)
    tracker.transform(
        _frequency(50.3, seconds=1_726_000_100, nanos=123_456_789),
        FREQUENCY_MRID.encode(),
    )
    envelope = tracker.transform(
        _frequency(50.0, seconds=1_726_000_101, nanos=987_654_321),
        FREQUENCY_MRID.encode(),
    )
    assert envelope is not None
    return envelope


def _frequency(
    value: float,
    *,
    seconds: int = 1_726_000_000,
    nanos: int = 0,
) -> rtd_schema_pb2.MCCSMeasurementValue:
    source = rtd_schema_pb2.MCCSMeasurementValue(
        mrid=FREQUENCY_MRID,
        double_value=value,
    )
    source.timestamp_mccs.seconds = seconds
    source.timestamp_mccs.nanos = nanos
    source.quality.valid = True
    return source


def _integer_frequency(*, seconds: int) -> rtd_schema_pb2.MCCSMeasurementValue:
    source = _frequency(50.3, seconds=seconds)
    source.int_value = 50
    return source


def _without_quality_valid(*, seconds: int) -> rtd_schema_pb2.MCCSMeasurementValue:
    source = _frequency(50.3, seconds=seconds)
    source.quality.ClearField("valid")
    return source


def _frequency_with_quality_flag(
    field: str,
    *,
    seconds: int,
) -> rtd_schema_pb2.MCCSMeasurementValue:
    source = _frequency(50.3, seconds=seconds)
    setattr(source.quality, field, True)
    return source


if __name__ == "__main__":
    unittest.main()