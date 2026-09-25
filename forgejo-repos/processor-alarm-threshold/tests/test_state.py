"""Tests for threshold transitions and current compacted Alarm state."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import unittest

from processor_alarm_threshold.alarm import AlarmContractError, canonical_alarm_key
from processor_alarm_threshold.generated import alarm_pb2, rtd_schema_pb2
from processor_alarm_threshold.state import StateOutputs, ThresholdState

from support import CATALOG_ID, FREQUENCY_MRID, measurement, reviewed_rules, source


NOW = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)


class ThresholdStateTests(unittest.TestCase):
    """State changes only after current, valid, newer observations."""

    def test_zero_membership_completes_ready_idle_without_an_alarm(self) -> None:
        state = ThresholdState(CATALOG_ID, reviewed_rules())

        self.assertEqual(state.complete_initial_snapshot(0), StateOutputs())
        self.assertEqual(state.member_mrids, frozenset())
        self.assertEqual(state.active_alarm_keys, frozenset())

    def test_new_member_waits_for_its_first_qualifying_live_measurement(self) -> None:
        state = ThresholdState(CATALOG_ID, reviewed_rules())
        state.complete_initial_snapshot(0)

        added = state.apply_masterdata(b"pmu-bay-01", source("pmu-bay-01"), 1)

        self.assertEqual(added, StateOutputs())
        self.assertEqual(state.active_alarm_keys, frozenset())
        outputs = self._evaluate(state, 50.2, NOW, offset=1)
        self.assertEqual(len(outputs.alarm_outputs), 1)
        self.assertEqual(len(outputs.watermark_outputs), 1)

    def test_snapshot_folding_preserves_active_state_and_same_key_tombstones_clear_it(self) -> None:
        initial = self._state_with_member()
        activation = self._evaluate(initial, 50.2, NOW, offset=10)
        active = activation.alarm_outputs[0]
        restored = self._state_with_member()

        restored.fold_alarm_snapshot(active.key, active.value)
        restored.fold_watermark_snapshot(
            activation.watermark_outputs[0].key,
            activation.watermark_outputs[0].value,
        )
        self.assertEqual(restored.complete_initial_snapshot(0), StateOutputs())
        self.assertEqual(restored.active_alarm_keys, frozenset({canonical_alarm_key("frequency-over-50-1-hz", FREQUENCY_MRID)}))

        restored.apply_alarm(active.key, None)

        self.assertEqual(restored.active_alarm_keys, frozenset())

    def test_snapshot_tombstone_preserves_an_inactive_watermark_after_restart(self) -> None:
        initial = self._state_with_member()
        activation = self._evaluate(initial, 50.2, NOW, offset=10)
        clear = self._evaluate(initial, 49.0, NOW + timedelta(seconds=10), offset=11)
        restored = self._state_with_member()

        self.assertIsNone(clear.alarm_outputs[0].value)
        restored.fold_watermark_snapshot(
            clear.watermark_outputs[0].key,
            clear.watermark_outputs[0].value,
        )
        self.assertEqual(
            restored.complete_initial_snapshot(clear.watermark_outputs[0].timestamp_ms),
            StateOutputs(),
        )

        delayed = self._evaluate(restored, 50.2, NOW + timedelta(seconds=5), offset=12)
        newer = self._evaluate(restored, 50.2, NOW + timedelta(seconds=11), offset=13)

        self.assertEqual(delayed, StateOutputs())
        self.assertEqual(len(newer.alarm_outputs), 1)
        self.assertEqual(len(newer.watermark_outputs), 1)
        self.assertNotEqual(
            _alarm(newer.alarm_outputs[0]).episode_id,
            _alarm(activation.alarm_outputs[0]).episode_id,
        )

    def test_threshold_quality_freshness_and_out_of_order_records_leave_or_change_state_correctly(self) -> None:
        state = self._state_with_member()
        stale = self._evaluate(state, 50.2, NOW - timedelta(seconds=31), offset=1)
        missing_valid = self._evaluate(state, 50.2, NOW, offset=2, valid=None)
        active = self._evaluate(state, 50.2, NOW, offset=3)
        active_message = _alarm(active.alarm_outputs[0])
        non_finite = self._evaluate(state, float("nan"), NOW + timedelta(seconds=1), offset=4)
        bad_quality = self._evaluate(
            state,
            49.0,
            NOW + timedelta(seconds=1),
            offset=5,
            substituted=True,
        )
        out_of_order_clear = self._evaluate(state, 49.0, NOW, offset=6)
        refresh = self._evaluate(state, 50.3, NOW + timedelta(seconds=2), offset=7)
        refresh_message = _alarm(refresh.alarm_outputs[0])
        clear = self._evaluate(state, 50.1, NOW + timedelta(seconds=3), offset=8)
        reactivate = self._evaluate(state, 50.4, NOW + timedelta(seconds=4), offset=9)
        reactivate_message = _alarm(reactivate.alarm_outputs[0])

        self.assertEqual(stale, StateOutputs())
        self.assertEqual(missing_valid, StateOutputs())
        self.assertEqual(len(active.alarm_outputs), 1)
        self.assertEqual(len(active.watermark_outputs), 1)
        self.assertEqual(non_finite, StateOutputs())
        self.assertEqual(bad_quality, StateOutputs())
        self.assertEqual(out_of_order_clear, StateOutputs())
        self.assertEqual(len(refresh.alarm_outputs), 1)
        self.assertEqual(refresh_message.episode_id, active_message.episode_id)
        self.assertEqual(clear.alarm_outputs[0].key, active.alarm_outputs[0].key)
        self.assertIsNone(clear.alarm_outputs[0].value)
        self.assertNotEqual(reactivate_message.episode_id, active_message.episode_id)

    def test_key_mismatch_and_non_newer_records_do_not_change_an_active_condition(self) -> None:
        state = self._state_with_member()
        active = self._evaluate(state, 50.2, NOW, offset=10)
        mismatch = state.evaluate_live_measurement(
            key=b"wrong-key",
            value=measurement(value=49.0, observed_at=NOW + timedelta(seconds=1)),
            topic="LiveMeasurement",
            partition=0,
            offset=11,
            now=NOW + timedelta(seconds=1),
        )
        duplicate = self._evaluate(state, 49.0, NOW, offset=12)

        self.assertEqual(len(active.alarm_outputs), 1)
        self.assertEqual(mismatch, StateOutputs())
        self.assertEqual(duplicate, StateOutputs())
        self.assertEqual(len(state.active_alarm_keys), 1)

    def test_membership_removal_emits_a_same_key_tombstone_for_active_conditions(self) -> None:
        state = self._state_with_member()
        activation = self._evaluate(state, 50.2, NOW, offset=10)
        active = activation.alarm_outputs[0]

        removed = state.apply_masterdata(b"pmu-bay-01", None, 1234)

        self.assertEqual(len(removed.alarm_outputs), 1)
        self.assertEqual(removed.alarm_outputs[0].key, active.key)
        self.assertIsNone(removed.alarm_outputs[0].value)
        self.assertEqual(removed.watermark_outputs, ())
        self.assertEqual(state.member_mrids, frozenset())

    def test_newer_inactive_measurement_writes_only_an_exact_watermark(self) -> None:
        state = self._state_with_member()
        raw_measurement = rtd_schema_pb2.MCCSMeasurementValue()
        raw_measurement.ParseFromString(measurement(value=49.0, observed_at=NOW))
        raw_measurement.timestamp_mccs.nanos = 123_456_789

        outputs = state.evaluate_live_measurement(
            key=FREQUENCY_MRID.encode("utf-8"),
            value=raw_measurement.SerializeToString(),
            topic="LiveMeasurement",
            partition=0,
            offset=10,
            now=NOW,
        )

        self.assertEqual(outputs.alarm_outputs, ())
        self.assertEqual(len(outputs.watermark_outputs), 1)
        watermark = _watermark(outputs.watermark_outputs[0])
        self.assertEqual(watermark.last_evaluated_at.seconds, raw_measurement.timestamp_mccs.seconds)
        self.assertEqual(watermark.last_evaluated_at.nanos, raw_measurement.timestamp_mccs.nanos)

    def test_membership_removal_retains_watermark_when_member_is_readded(self) -> None:
        state = self._state_with_member()
        activation = self._evaluate(state, 50.2, NOW, offset=10)

        removed = state.apply_masterdata(b"pmu-bay-01", None, 1_234)
        readded = state.apply_masterdata(b"pmu-bay-01", source("pmu-bay-01"), 1_235)
        delayed = self._evaluate(state, 50.2, NOW, offset=11)
        newer = self._evaluate(state, 50.2, NOW + timedelta(seconds=1), offset=12)

        self.assertEqual(len(removed.alarm_outputs), 1)
        self.assertIsNone(removed.alarm_outputs[0].value)
        self.assertEqual(removed.watermark_outputs, ())
        self.assertEqual(readded, StateOutputs())
        self.assertEqual(delayed, StateOutputs())
        self.assertEqual(len(newer.alarm_outputs), 1)
        self.assertNotEqual(
            _alarm(newer.alarm_outputs[0]).episode_id,
            _alarm(activation.alarm_outputs[0]).episode_id,
        )

    def test_rule_revision_preserves_watermark_and_active_episode(self) -> None:
        initial = self._state_with_member()
        activation = self._evaluate(initial, 50.2, NOW, offset=10)
        revised_rules = (replace(reviewed_rules()[0], revision="2"),)
        restored = ThresholdState(CATALOG_ID, revised_rules)
        restored.fold_masterdata_snapshot(b"pmu-bay-01", source("pmu-bay-01"))
        restored.fold_alarm_snapshot(
            activation.alarm_outputs[0].key,
            activation.alarm_outputs[0].value,
        )
        restored.fold_watermark_snapshot(
            activation.watermark_outputs[0].key,
            activation.watermark_outputs[0].value,
        )

        self.assertEqual(restored.complete_initial_snapshot(0), StateOutputs())
        self.assertEqual(self._evaluate(restored, 50.3, NOW, offset=11), StateOutputs())
        refreshed = self._evaluate(restored, 50.3, NOW + timedelta(seconds=1), offset=12)

        self.assertEqual(_alarm(refreshed.alarm_outputs[0]).rule_revision, "2")
        self.assertEqual(
            _alarm(refreshed.alarm_outputs[0]).episode_id,
            _alarm(activation.alarm_outputs[0]).episode_id,
        )

    def test_active_alarm_without_watermark_requires_explicit_migration_fence(self) -> None:
        initial = self._state_with_member()
        activation = self._evaluate(initial, 50.2, NOW, offset=10)
        blocked = self._state_with_member()
        blocked.fold_alarm_snapshot(
            activation.alarm_outputs[0].key,
            activation.alarm_outputs[0].value,
        )

        with self.assertRaisesRegex(AlarmContractError, "no AlarmEvaluationWatermark"):
            blocked.complete_initial_snapshot(0)

        fenced = ThresholdState(
            CATALOG_ID,
            reviewed_rules(),
            allow_legacy_active_alarm_bootstrap=True,
        )
        fenced.fold_masterdata_snapshot(b"pmu-bay-01", source("pmu-bay-01"))
        fenced.fold_alarm_snapshot(
            activation.alarm_outputs[0].key,
            activation.alarm_outputs[0].value,
        )
        bootstrap = fenced.complete_initial_snapshot(0)

        self.assertEqual(bootstrap.alarm_outputs, ())
        self.assertEqual(len(bootstrap.watermark_outputs), 1)
        self.assertEqual(
            fenced.active_alarm_keys,
            frozenset({canonical_alarm_key("frequency-over-50-1-hz", FREQUENCY_MRID)}),
        )

    def test_watermark_tombstone_does_not_establish_a_high_water(self) -> None:
        state = self._state_with_member()
        key = canonical_alarm_key("frequency-over-50-1-hz", FREQUENCY_MRID).encode("utf-8")
        state.fold_watermark_snapshot(key, None)
        self.assertEqual(state.complete_initial_snapshot(0), StateOutputs())

        activation = self._evaluate(state, 50.2, NOW, offset=10)

        self.assertEqual(len(activation.alarm_outputs), 1)
        self.assertEqual(len(activation.watermark_outputs), 1)

    @staticmethod
    def _state_with_member() -> ThresholdState:
        state = ThresholdState(CATALOG_ID, reviewed_rules())
        state.fold_masterdata_snapshot(b"pmu-bay-01", source("pmu-bay-01"))
        state.complete_initial_snapshot(0)
        return state

    @staticmethod
    def _evaluate(
        state: ThresholdState,
        value: float,
        observed_at: datetime,
        *,
        offset: int,
        valid: bool | None = True,
        substituted: bool | None = None,
    ) -> StateOutputs:
        return state.evaluate_live_measurement(
            key=FREQUENCY_MRID.encode("utf-8"),
            value=measurement(
                value=value,
                observed_at=observed_at,
                valid=valid,
                substituted=substituted,
            ),
            topic="LiveMeasurement",
            partition=0,
            offset=offset,
            now=NOW,
        )


def _alarm(output: object) -> alarm_pb2.AlarmDesiredState:
    message = alarm_pb2.AlarmDesiredState()
    message.ParseFromString(output.value)
    return message


def _watermark(output: object) -> alarm_pb2.AlarmEvaluationWatermark:
    message = alarm_pb2.AlarmEvaluationWatermark()
    message.ParseFromString(output.value)
    return message


if __name__ == "__main__":
    unittest.main()