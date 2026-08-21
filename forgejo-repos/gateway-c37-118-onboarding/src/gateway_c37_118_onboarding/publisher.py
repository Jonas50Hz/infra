"""One-shot, acknowledged reconciliation of Masterdata into Kafka."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import os
from time import monotonic, sleep
from typing import Any

from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.errors import KafkaError

from gateway_c37_118_onboarding.config import CatalogError, load_catalog
from gateway_c37_118_onboarding.reconciliation import (
    ReconciliationError,
    ReconciliationPlan,
    reconcile_catalog,
)


class PublisherError(RuntimeError):
    """Raised when the compacted runtime projection cannot be reconciled."""


@dataclass(frozen=True)
class Settings:
    """Environment-backed publisher configuration for an approved catalog revision."""

    kafka_bootstrap_servers: str
    catalog_directory: str
    catalog_id: str
    catalog_revision: str
    topic: str

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> Settings:
        """Read the narrow set of deployment-time configuration values."""

        values = os.environ if environment is None else environment
        return cls(
            kafka_bootstrap_servers=_required(
                values,
                "KAFKA_BOOTSTRAP_SERVERS",
                "kafka:9092",
            ),
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
                "development",
            ),
            topic=_required(values, "WAMA_MASTERDATA_TOPIC", "Masterdata"),
        )


def reconcile(settings: Settings) -> ReconciliationPlan:
    """Read compacted state, validate the catalog, and publish one safe update plan."""

    try:
        catalog = load_catalog(
            settings.catalog_directory,
            settings.catalog_id,
            settings.catalog_revision,
        )
    except CatalogError as error:
        raise PublisherError(f"Masterdata catalog is invalid: {error}") from error

    consumer = KafkaConsumer(
        bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
        client_id="wama-masterdata-publisher-read",
        enable_auto_commit=False,
        group_id=None,
        request_timeout_ms=30_000,
        api_version_auto_timeout_ms=10_000,
    )
    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
        client_id="wama-masterdata-publisher-write",
        acks="all",
        retries=5,
        request_timeout_ms=30_000,
    )
    try:
        existing = read_compacted_state(consumer, settings.topic)
        published_at = datetime.now(timezone.utc)
        plan = reconcile_catalog(catalog, existing, published_at)
        publish_plan(producer, settings.topic, plan, published_at)
        return plan
    except (KafkaError, ReconciliationError) as error:
        raise PublisherError(f"Masterdata reconciliation failed: {error}") from error
    finally:
        consumer.close(autocommit=False)
        producer.close(timeout=30)


def read_compacted_state(
    consumer: Any,
    topic: str,
    timeout_seconds: float = 30.0,
) -> dict[str, bytes | None]:
    """Read every current key from a compacted topic through its end offsets."""

    deadline = monotonic() + timeout_seconds
    partitions = consumer.partitions_for_topic(topic)
    while partitions is None:
        if monotonic() >= deadline:
            raise PublisherError(f"Kafka did not expose topic {topic!r}")
        sleep(0.1)
        partitions = consumer.partitions_for_topic(topic)
    assignments = {TopicPartition(topic, partition) for partition in partitions}
    if not assignments:
        raise PublisherError(f"Kafka topic {topic!r} has no partitions")
    consumer.assign(assignments)
    consumer.seek_to_beginning(*assignments)
    end_offsets = consumer.end_offsets(assignments)
    state: dict[str, bytes | None] = {}
    while any(consumer.position(partition) < end_offsets[partition] for partition in assignments):
        if monotonic() >= deadline:
            raise PublisherError(f"Timed out reading compacted topic {topic!r}")
        records = consumer.poll(timeout_ms=1_000)
        for partition_records in records.values():
            for record in partition_records:
                state[_record_key(record.key)] = record.value
    return state


def publish_plan(
    producer: Any,
    topic: str,
    plan: ReconciliationPlan,
    published_at: datetime,
) -> int:
    """Await Kafka acknowledgement for each source upsert and tombstone."""

    timestamp_ms = int(published_at.timestamp() * 1_000)
    count = 0
    for record in plan.upserts:
        producer.send(
            topic,
            key=record.source_id.encode("utf-8"),
            value=record.payload,
            timestamp_ms=timestamp_ms,
        ).get(timeout=30)
        print(f"upserted Masterdata source {record.source_id}")
        count += 1
    for source_id in plan.tombstones:
        producer.send(
            topic,
            key=source_id.encode("utf-8"),
            value=None,
            timestamp_ms=timestamp_ms,
        ).get(timeout=30)
        print(f"tombstoned Masterdata source {source_id}")
        count += 1
    return count


def _record_key(key: object) -> str:
    if not isinstance(key, bytes) or not key:
        raise PublisherError("Compacted Masterdata record has no UTF-8 source key")
    try:
        decoded = key.decode("utf-8")
    except UnicodeDecodeError as error:
        raise PublisherError("Compacted Masterdata key is not UTF-8") from error
    if not decoded:
        raise PublisherError("Compacted Masterdata record has an empty source key")
    return decoded


def _required(values: Mapping[str, str], name: str, default: str) -> str:
    value = values.get(name, default).strip()
    if not value:
        raise PublisherError(f"{name} must be configured")
    return value