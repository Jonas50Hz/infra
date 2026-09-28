"""Bounded proof that every reviewed catalog MRID reaches LiveMeasurement."""

from __future__ import annotations

import os
from time import monotonic
from typing import Optional
from uuid import uuid4

from kafka import KafkaConsumer, TopicPartition

from gateway_c37_118.catalog import load_catalog
from gateway_c37_118.generated import rtd_schema_pb2


class VerificationError(ValueError):
    """Raised when a catalog-derived LiveMeasurement record is malformed."""


def verify_record(
    expected_mrids: frozenset[str], key: Optional[bytes], value: Optional[bytes]
) -> Optional[str]:
    """Validate one candidate record and return its MRID if it is expected."""

    if key is None or value is None:
        return None
    record = rtd_schema_pb2.MCCSMeasurementValue()
    try:
        record.ParseFromString(value)
    except Exception as error:
        raise VerificationError("LiveMeasurement is not raw Protobuf") from error
    if record.mrid not in expected_mrids:
        return None
    if key != record.mrid.encode("utf-8"):
        raise VerificationError(f"LiveMeasurement Kafka key does not match {record.mrid}")
    if record.WhichOneof("value") != "double_value":
        raise VerificationError(f"{record.mrid} does not contain a double_value")
    if not record.HasField("quality") or not record.quality.HasField("valid"):
        raise VerificationError(f"{record.mrid} has no explicit quality.valid")
    if (
        not record.HasField("timestamp_field")
        or not record.HasField("timestamp_gateway")
        or not record.HasField("timestamp_mccs")
    ):
        raise VerificationError(f"{record.mrid} has incomplete timestamps")
    field = (record.timestamp_field.seconds, record.timestamp_field.nanos)
    gateway = (record.timestamp_gateway.seconds, record.timestamp_gateway.nanos)
    mccs = (record.timestamp_mccs.seconds, record.timestamp_mccs.nanos)
    if any(nanos < 0 or nanos >= 1_000_000_000 for _, nanos in (field, gateway, mccs)):
        raise VerificationError(f"{record.mrid} has an invalid timestamp")
    if not field <= gateway <= mccs:
        raise VerificationError(f"{record.mrid} timestamps are not field <= gateway <= MCCS")
    return record.mrid


def main() -> None:
    """Consume from current end offsets until all approved MRIDs are proved."""

    catalog = load_catalog(os.environ.get("WAMA_CATALOG_DIR", "/app/catalog"))
    expected = catalog.mrids
    topic = os.environ.get("WAMA_LIVE_MEASUREMENT_TOPIC", "LiveMeasurement")
    timeout_seconds = float(os.environ.get("WAMA_VERIFY_TIMEOUT_SECONDS", "30"))
    consumer = KafkaConsumer(
        bootstrap_servers=os.environ.get("WAMA_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").split(","),
        group_id=f"c37-118-verify-{uuid4()}",
        enable_auto_commit=False,
        auto_offset_reset="latest",
        consumer_timeout_ms=500,
    )
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        raise SystemExit(f"{topic} does not exist")
    assignments = [TopicPartition(topic, partition) for partition in sorted(partitions)]
    consumer.assign(assignments)
    consumer.seek_to_end(*assignments)
    seen: set[str] = set()
    deadline = monotonic() + timeout_seconds
    try:
        while monotonic() < deadline and seen != expected:
            for records in consumer.poll(timeout_ms=500).values():
                for record in records:
                    mrid = verify_record(expected, record.key, record.value)
                    if mrid:
                        seen.add(mrid)
    finally:
        consumer.close()
    missing = sorted(expected.difference(seen))
    if missing:
        raise SystemExit(f"Timed out waiting for catalog MRIDs: {', '.join(missing)}")
    print(f"Verified {len(seen)} catalog LiveMeasurement MRIDs.")


if __name__ == "__main__":
    main()
