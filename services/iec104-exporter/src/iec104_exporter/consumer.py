"""Kafka worker that commits Export records only after c104 accepts delivery."""

from __future__ import annotations

import logging
from threading import Event, Thread
from collections.abc import Callable
from typing import Any

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
from kafka.structs import OffsetAndMetadata

from iec104_common.contract import ContractValidationError, parse_kafka_record
from iec104_exporter.config import Settings
from iec104_exporter.transport import Iec104Transport, Iec104TransportError

LOGGER = logging.getLogger(__name__)


class ExportWorker:
    """Replay Export records until a connected control center accepts each one."""

    def __init__(
        self,
        settings: Settings,
        transport: Iec104Transport,
        consumer_factory: Callable[..., Any] = KafkaConsumer,
    ) -> None:
        self._settings = settings
        self._transport = transport
        self._consumer_factory = consumer_factory
        self._ready = Event()
        self._stop = Event()
        self._thread: Thread | None = None
        self.failure: str | None = None

    @property
    def ready(self) -> bool:
        """Whether the worker owns a live Kafka consumer."""

        return self._ready.is_set()

    def start(self) -> None:
        """Run the consumer loop in one background thread."""

        if self._thread is not None:
            return
        self._thread = Thread(target=self._run, name="iec104-exporter", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop the worker and wait briefly for Kafka cleanup."""

        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def process_record(self, record: Any) -> bool:
        """Decode one keyed record and request its outbound IEC 104 delivery."""

        return self._transport.publish(parse_kafka_record(record))

    def _run(self) -> None:
        while not self._stop.is_set():
            consumer: Any | None = None
            try:
                consumer = self._consumer_factory(
                    self._settings.kafka_topic,
                    bootstrap_servers=self._settings.kafka_bootstrap_servers.split(","),
                    client_id="iec104-exporter",
                    group_id=self._settings.kafka_consumer_group,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                    request_timeout_ms=30_000,
                    api_version_auto_timeout_ms=10_000,
                )
                self.failure = None
                self._ready.set()
                self._consume(consumer)
            except (KafkaError, OSError) as error:
                self._ready.clear()
                self.failure = f"{type(error).__name__}: {error}"
                LOGGER.warning("IEC 104 Kafka consumption is unavailable; retrying: %s", self.failure)
                self._stop.wait(self._settings.retry_interval_seconds)
            except (ContractValidationError, Iec104TransportError, ValueError) as error:
                self._ready.clear()
                self.failure = f"{type(error).__name__}: {error}"
                LOGGER.exception("IEC 104 export stopped")
                return
            finally:
                if consumer is not None:
                    consumer.close(autocommit=False)

    def _consume(self, consumer: Any) -> None:
        while not self._stop.is_set():
            if not self._transport.active_control_center:
                self._stop.wait(self._settings.retry_interval_seconds)
                continue
            retry_record = False
            records = consumer.poll(timeout_ms=1_000)
            for partition_records in records.values():
                for record in partition_records:
                    if self.process_record(record):
                        consumer.commit(
                            {
                                TopicPartition(record.topic, record.partition): OffsetAndMetadata(
                                    record.offset + 1,
                                    "",
                                    -1,
                                )
                            }
                        )
                        continue
                    consumer.seek(TopicPartition(record.topic, record.partition), record.offset)
                    retry_record = True
                    break
                if retry_record:
                    break
            if retry_record:
                self._stop.wait(self._settings.retry_interval_seconds)