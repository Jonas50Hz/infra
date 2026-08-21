"""Tests for bounded catalog-derived LiveMeasurement verification."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import unittest

from gateway_c37_118_onboarding.generated import rtd_schema_pb2
from gateway_c37_118_onboarding.verify_live_measurements import (
    LiveMeasurementVerificationError,
    VerificationSettings,
    _observe_record,
    expected_mrids,
    verify,
)


class LiveMeasurementVerificationTests(unittest.TestCase):
    """Require one fresh well-formed raw-Protobuf record for every reviewed MRID."""

    def test_verifies_every_shipped_catalog_mrid(self) -> None:
        settings = _settings()
        expected = expected_mrids(settings)
        consumer = _Consumer([{
            "partition": [_record(mrid) for mrid in sorted(expected)],
        }])

        count = verify(settings, consumer_factory=lambda *args, **kwargs: consumer, clock=lambda: 0.0)

        self.assertEqual(count, len(expected))
        self.assertFalse(consumer.autocommit)

    def test_ignores_unrelated_topic_traffic(self) -> None:
        expected = frozenset({"urn:wama:test:expected"})
        observed: set[str] = set()

        _observe_record(_record("urn:wama:test:unrelated"), expected, observed)
        _observe_record(_record("urn:wama:test:expected"), expected, observed)

        self.assertEqual(observed, {"urn:wama:test:expected"})

    def test_rejects_invalid_expected_payload(self) -> None:
        expected_mrid = "urn:wama:test:expected"

        with self.assertRaisesRegex(LiveMeasurementVerificationError, "not valid MCCSMeasurementValue"):
            _observe_record(
                SimpleNamespace(key=expected_mrid.encode("utf-8"), value=b"not-protobuf", timestamp=0),
                frozenset({expected_mrid}),
                set(),
            )

    def test_rejects_expected_record_with_wrong_key(self) -> None:
        expected_mrid = "urn:wama:test:expected"

        with self.assertRaisesRegex(LiveMeasurementVerificationError, "Kafka key"):
            _observe_record(
                _record(expected_mrid, key=b"urn:wama:test:wrong"),
                frozenset({expected_mrid}),
                set(),
            )

    def test_rejects_expected_record_without_a_double_value(self) -> None:
        expected_mrid = "urn:wama:test:expected"

        with self.assertRaisesRegex(LiveMeasurementVerificationError, "no double value"):
            _observe_record(
                _record(expected_mrid, value_kind="bool"),
                frozenset({expected_mrid}),
                set(),
            )

    def test_accepts_expected_record_with_explicit_invalid_quality(self) -> None:
        expected_mrid = "urn:wama:test:expected"
        observed: set[str] = set()

        _observe_record(
            _record(expected_mrid, quality_valid=False),
            frozenset({expected_mrid}),
            observed,
        )

        self.assertEqual(observed, {expected_mrid})

    def test_reports_missing_mrids_at_timeout(self) -> None:
        settings = _settings(timeout_seconds=1.0)
        consumer = _Consumer([{}])
        clock_values = iter((0.0, 0.0, 1.0))

        with self.assertRaisesRegex(LiveMeasurementVerificationError, "missing MRIDs"):
            verify(
                settings,
                consumer_factory=lambda *args, **kwargs: consumer,
                clock=lambda: next(clock_values),
            )

        self.assertFalse(consumer.autocommit)


class _Consumer:
    def __init__(self, batches: list[dict[str, list[SimpleNamespace]]]) -> None:
        self._batches = batches
        self.autocommit: bool | None = None

    def close(self, autocommit: bool) -> None:
        self.autocommit = autocommit

    def poll(self, timeout_ms: int) -> dict[str, list[SimpleNamespace]]:
        return self._batches.pop(0) if self._batches else {}


def _settings(timeout_seconds: float = 30.0) -> VerificationSettings:
    return VerificationSettings(
        kafka_bootstrap_servers="kafka:9092",
        catalog_directory=str(Path(__file__).resolve().parents[1] / "catalog" / "sources"),
        catalog_id="wama-c37-118-onboarding",
        catalog_revision="test",
        live_measurement_topic="LiveMeasurement",
        timeout_seconds=timeout_seconds,
    )


def _record(
    mrid: str,
    key: bytes | None = None,
    value_kind: str = "double",
    quality_valid: bool = True,
) -> SimpleNamespace:
    measurement = rtd_schema_pb2.MCCSMeasurementValue(mrid=mrid)
    if value_kind == "double":
        measurement.double_value = 50.01
    elif value_kind == "bool":
        measurement.bool_value = True
    else:
        raise ValueError(f"Unsupported test value kind: {value_kind}")
    measurement.quality.valid = quality_valid
    measurement.timestamp_field.seconds = 1_700_000_000
    measurement.timestamp_field.nanos = 500_000_000
    measurement.timestamp_gateway.seconds = 1_700_000_000
    measurement.timestamp_gateway.nanos = 600_000_000
    measurement.timestamp_mccs.seconds = 1_700_000_000
    measurement.timestamp_mccs.nanos = 600_000_000
    return SimpleNamespace(
        key=mrid.encode("utf-8") if key is None else key,
        value=measurement.SerializeToString(),
        timestamp=1_700_000_000_600,
    )