"""Bounded verification of reviewed C37.118 measurements on Kafka."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import math
import os
import sys
from time import monotonic
from typing import Any
from uuid import uuid4

from google.protobuf.message import DecodeError
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from gateway_c37_118_onboarding.config import CatalogError, load_catalog
from gateway_c37_118_onboarding.generated import rtd_schema_pb2


class LiveMeasurementVerificationError(RuntimeError):
    """Raised when the reviewed source measurements cannot be verified safely."""


@dataclass(frozen=True)
class VerificationSettings:
    """Environment-backed configuration for one bounded Kafka verification run."""

    kafka_bootstrap_servers: str
    catalog_directory: str
    catalog_id: str
    catalog_revision: str
    live_measurement_topic: str
    timeout_seconds: float

    @classmethod
    def from_environment(
        cls,
        environment: Mapping[str, str] | None = None,
    ) -> VerificationSettings:
        """Read only the catalog and Kafka inputs required for verification."""

        values = os.environ if environment is None else environment
        return cls(
            kafka_bootstrap_servers=_required(values, "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            catalog_directory=_required(
                values,
                "WAMA_MASTERDATA_CATALOG_DIRECTORY",
                "/app/catalog/sources",
            ),
            catalog_id=_required(
                values,
                "WAMA_MASTERDATA_CATALOG_ID",
                "wama-c37-118-onboarding",
            ),
            catalog_revision=_required(
                values,
                "WAMA_MASTERDATA_CATALOG_REVISION",
                "runtime",
            ),
            live_measurement_topic=_required(
                values,
                "WAMA_LIVE_MEASUREMENT_TOPIC",
                "LiveMeasurement",
            ),
            timeout_seconds=_positive_float(
                values,
                "WAMA_LIVE_MEASUREMENT_VERIFY_TIMEOUT_SECONDS",
                "30",
            ),
        )


def expected_mrids(settings: VerificationSettings) -> frozenset[str]:
    """Derive the exact immutable MRID set from the approved catalog revision."""

    try:
        catalog = load_catalog(
            settings.catalog_directory,
            settings.catalog_id,
            settings.catalog_revision,
        )
    except CatalogError as error:
        raise LiveMeasurementVerificationError(f"Masterdata catalog is invalid: {error}") from error
    mrids = frozenset(
        signal.mrid
        for source in catalog.sources
        for signal in source.signals
    )
    if not mrids:
        raise LiveMeasurementVerificationError("Masterdata catalog has no signals to verify")
    return mrids


def verify(
    settings: VerificationSettings,
    consumer_factory: Callable[..., Any] = KafkaConsumer,
    clock: Callable[[], float] = monotonic,
) -> int:
    """Observe one fresh well-formed record for every reviewed catalog MRID."""

    expected = expected_mrids(settings)
    observed: set[str] = set()
    consumer: Any | None = None
    try:
        verifier_id = uuid4().hex
        consumer = consumer_factory(
            settings.live_measurement_topic,
            bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
            client_id=f"wama-c37-118-live-measurement-verifier-{verifier_id}",
            group_id=f"wama-c37-118-live-measurement-verifier-{verifier_id}",
            enable_auto_commit=False,
            auto_offset_reset="latest",
            request_timeout_ms=30_000,
            api_version_auto_timeout_ms=10_000,
        )
        deadline = clock() + settings.timeout_seconds
        while observed != expected:
            remaining_seconds = deadline - clock()
            if remaining_seconds <= 0:
                raise _missing_mrids_error(expected, observed)
            records = consumer.poll(timeout_ms=min(1_000, max(1, int(remaining_seconds * 1_000))))
            for partition_records in records.values():
                for record in partition_records:
                    _observe_record(record, expected, observed)
        return len(observed)
    except KafkaError as error:
        raise LiveMeasurementVerificationError(f"Kafka LiveMeasurement verification failed: {error}") from error
    finally:
        if consumer is not None:
            consumer.close(autocommit=False)


def _observe_record(record: Any, expected: frozenset[str], observed: set[str]) -> None:
    key = getattr(record, "key", None)
    expected_key = key in {mrid.encode("utf-8") for mrid in expected}
    measurement = rtd_schema_pb2.MCCSMeasurementValue()
    try:
        measurement.ParseFromString(getattr(record, "value", None))
    except (DecodeError, TypeError) as error:
        if expected_key:
            raise LiveMeasurementVerificationError(
                "LiveMeasurement payload is not valid MCCSMeasurementValue Protobuf"
            ) from error
        return

    if measurement.mrid not in expected:
        if expected_key:
            raise LiveMeasurementVerificationError(
                "LiveMeasurement Kafka key does not match its MCCS MRID"
            )
        return
    _validate_measurement(record, measurement)
    observed.add(measurement.mrid)


def _validate_measurement(record: Any, measurement: Any) -> None:
    if record.key != measurement.mrid.encode("utf-8"):
        raise LiveMeasurementVerificationError("LiveMeasurement Kafka key does not match its MCCS MRID")
    if measurement.WhichOneof("value") != "double_value":
        raise LiveMeasurementVerificationError("LiveMeasurement MCCSMeasurementValue has no double value")
    if not math.isfinite(measurement.double_value):
        raise LiveMeasurementVerificationError("LiveMeasurement MCCSMeasurementValue has a non-finite double value")
    if not measurement.HasField("quality") or not measurement.quality.HasField("valid"):
        raise LiveMeasurementVerificationError(
            "LiveMeasurement MCCSMeasurementValue has no explicit valid quality flag"
        )

    timestamp_names = ("timestamp_field", "timestamp_gateway", "timestamp_mccs")
    for timestamp_name in timestamp_names:
        if not measurement.HasField(timestamp_name):
            raise LiveMeasurementVerificationError(
                f"LiveMeasurement MCCSMeasurementValue has no {timestamp_name}"
            )
    field_timestamp = _timestamp_nanoseconds(measurement.timestamp_field)
    gateway_timestamp = _timestamp_nanoseconds(measurement.timestamp_gateway)
    mccs_timestamp = _timestamp_nanoseconds(measurement.timestamp_mccs)
    if not field_timestamp <= gateway_timestamp <= mccs_timestamp:
        raise LiveMeasurementVerificationError(
            "LiveMeasurement timestamps are not field <= gateway <= MCCS"
        )
    if getattr(record, "timestamp", None) != mccs_timestamp // 1_000_000:
        raise LiveMeasurementVerificationError(
            "LiveMeasurement Kafka timestamp does not match timestamp_mccs"
        )


def _timestamp_nanoseconds(timestamp: Any) -> int:
    if not 0 <= timestamp.nanos < 1_000_000_000:
        raise LiveMeasurementVerificationError("LiveMeasurement timestamp has invalid nanoseconds")
    return timestamp.seconds * 1_000_000_000 + timestamp.nanos


def _missing_mrids_error(expected: frozenset[str], observed: set[str]) -> LiveMeasurementVerificationError:
    missing = ", ".join(sorted(expected.difference(observed)))
    return LiveMeasurementVerificationError(
        f"LiveMeasurement verification timed out; missing MRIDs: {missing}"
    )


def _required(values: Mapping[str, str], name: str, default: str) -> str:
    value = values.get(name, default).strip()
    if not value:
        raise LiveMeasurementVerificationError(f"{name} must be configured")
    return value


def _positive_float(values: Mapping[str, str], name: str, default: str) -> float:
    value = _required(values, name, default)
    try:
        parsed = float(value)
    except ValueError as error:
        raise LiveMeasurementVerificationError(f"{name} must be a number") from error
    if not math.isfinite(parsed) or parsed <= 0:
        raise LiveMeasurementVerificationError(f"{name} must be a positive finite number")
    return parsed


def main() -> int:
    """Run one bounded catalog-derived Kafka verification."""

    try:
        count = verify(VerificationSettings.from_environment())
    except LiveMeasurementVerificationError as error:
        print(f"LiveMeasurement verification failed: {error}", file=sys.stderr)
        return 1
    print(f"LiveMeasurement verification completed: {count} MRID(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())