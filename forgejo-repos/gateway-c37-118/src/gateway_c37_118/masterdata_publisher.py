"""One-shot catalog validation and compacted Masterdata reconciliation."""

from __future__ import annotations

import os
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer, TopicPartition

from gateway_c37_118.catalog import load_catalog
from gateway_c37_118.masterdata import reconcile


def current_masterdata(bootstrap_servers: str, topic: str) -> dict[str, bytes]:
    """Read the compacted projection through the end offsets present at startup."""

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers.split(","),
        enable_auto_commit=False,
        consumer_timeout_ms=250,
        key_deserializer=lambda value: value.decode("utf-8") if value is not None else "",
    )
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        consumer.close()
        return {}
    assignments = [TopicPartition(topic, partition) for partition in sorted(partitions)]
    consumer.assign(assignments)
    ends = consumer.end_offsets(assignments)
    consumer.seek_to_beginning(*assignments)
    records: dict[str, bytes] = {}
    while any(consumer.position(partition) < ends[partition] for partition in assignments):
        batch = consumer.poll(timeout_ms=500)
        for partition_records in batch.values():
            for record in partition_records:
                if record.value is None:
                    records.pop(record.key, None)
                else:
                    records[record.key] = record.value
    consumer.close()
    return records


def main() -> None:
    """Publish the approved catalog once, waiting for all broker acknowledgements."""

    catalog = load_catalog(os.environ.get("WAMA_CATALOG_DIR", "/app/catalog"))
    topic = os.environ.get("WAMA_MASTERDATA_TOPIC", "Masterdata")
    bootstrap = os.environ.get("WAMA_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        acks="all",
        retries=10,
        client_id="c37-118-masterdata-publisher",
    )
    try:
        reconcile(
            producer,
            topic,
            catalog,
            os.environ.get("WAMA_CATALOG_REVISION", "local"),
            current_masterdata(bootstrap, topic),
            datetime.now(timezone.utc),
        )
    finally:
        producer.close()


if __name__ == "__main__":
    main()
