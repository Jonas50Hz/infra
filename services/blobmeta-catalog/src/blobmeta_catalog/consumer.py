"""Manual-commit Kafka materializer for immutable raw-Protobuf Blobmeta."""

from __future__ import annotations

import logging
from threading import Event
from collections.abc import Callable
from typing import Any

from google.protobuf.message import DecodeError
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import psycopg

from measurement_session_common.contract import ContractValidationError, validate_blobmeta, validate_kafka_key
from measurement_session_common.generated.blobmeta_pb2 import Blobmeta
from blobmeta_catalog.catalog import CatalogBlob, CatalogError, CatalogStore
from blobmeta_catalog.config import Settings

LOGGER = logging.getLogger(__name__)


class BlobmetaWorker:
    """Persist a Blobmeta row before manually committing its Kafka offset."""

    def __init__(
        self,
        settings: Settings,
        catalog: CatalogStore,
        consumer_factory: Callable[..., Any] = KafkaConsumer,
    ) -> None:
        self._settings = settings
        self._catalog = catalog
        self._consumer_factory = consumer_factory
        self._stop = Event()

    def run(self) -> None:
        """Reconnect transient broker/database faults without advancing offsets."""

        while not self._stop.is_set():
            consumer: Any | None = None
            try:
                consumer = self._consumer_factory(
                    self._settings.kafka_topic,
                    bootstrap_servers=self._settings.kafka_bootstrap_servers.split(","),
                    client_id="blobmeta-catalog",
                    group_id=self._settings.kafka_consumer_group,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                    request_timeout_ms=30_000,
                    api_version_auto_timeout_ms=10_000,
                )
                while not self._stop.is_set():
                    records = consumer.poll(timeout_ms=1_000)
                    for partition_records in records.values():
                        for record in partition_records:
                            self.process_record(record)
                            consumer.commit()
            except (KafkaError, OSError, psycopg.Error) as error:
                LOGGER.warning("Blobmeta catalog is unavailable; retrying: %s", error)
                self._stop.wait(self._settings.kafka_retry_interval_seconds)
            except (CatalogError, ContractValidationError, DecodeError, ValueError):
                LOGGER.exception("Blobmeta catalog stopped on invalid immutable evidence")
                raise
            finally:
                if consumer is not None:
                    consumer.close(autocommit=False)

    def process_record(self, record: Any) -> None:
        """Validate key-aligned wire bytes and insert their immutable projection."""

        message = Blobmeta()
        try:
            message.ParseFromString(record.value)
        except DecodeError as error:
            raise CatalogError("Blobmeta payload is not valid raw Protobuf") from error
        validate_blobmeta(message)
        validate_kafka_key(record.key, message.blob_id, "blob_id")
        self._catalog.insert(CatalogBlob.from_protobuf(message, record.value))