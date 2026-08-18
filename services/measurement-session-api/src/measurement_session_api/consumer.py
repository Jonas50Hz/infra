"""Kafka materializer for raw-Protobuf immutable finalized-session records."""

from __future__ import annotations

import logging
from threading import Event, Thread
from types import SimpleNamespace
from collections.abc import Callable
from typing import Any

from google.protobuf.message import DecodeError
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import psycopg

from measurement_session_common.contract import ContractValidationError, validate_measurement_session
from measurement_session_common.generated.measurement_session_pb2 import MeasurementSession
from measurement_session_api.catalog import CatalogError, CatalogSession, CatalogStore
from measurement_session_api.config import Settings

LOGGER = logging.getLogger(__name__)
DEFAULT_RETRY_INTERVAL_SECONDS = 2.0


class CatalogWorker:
    """Commit catalog rows before Kafka offsets for idempotent materialization."""

    def __init__(
        self,
        settings: Settings,
        catalog: CatalogStore,
        consumer_factory: Callable[..., Any] = KafkaConsumer,
        retry_interval_seconds: float = DEFAULT_RETRY_INTERVAL_SECONDS,
    ) -> None:
        self._settings = settings
        self._catalog = catalog
        self._consumer_factory = consumer_factory
        self._retry_interval_seconds = retry_interval_seconds
        self._stop = Event()
        self._ready = Event()
        self._thread: Thread | None = None
        self.failure: str | None = None

    @property
    def ready(self) -> bool:
        """Report whether the worker has a live Kafka consumer."""

        return self._ready.is_set()

    def start(self) -> None:
        """Start the bounded background consumer once the HTTP service is ready."""

        if self._thread is not None:
            return
        self._thread = Thread(target=self._run, name="measurement-session-catalog", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop polling and wait briefly for the consumer thread to exit."""

        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)

    def process_record(self, record: Any) -> None:
        """Validate one raw Kafka record and persist it before offset commit."""

        message = MeasurementSession()
        try:
            message.ParseFromString(record.value)
        except DecodeError as error:
            raise CatalogError("MeasurementSession payload is not valid raw Protobuf") from error
        validate_measurement_session(message)
        if record.key != message.session_id.encode("utf-8"):
            raise CatalogError("MeasurementSession Kafka key does not match session_id")
        self._catalog.insert(CatalogSession.from_protobuf(message, record.value))

    def _run(self) -> None:
        while not self._stop.is_set():
            consumer: Any | None = None
            try:
                consumer = self._consumer_factory(
                    self._settings.kafka_topic,
                    bootstrap_servers=self._settings.kafka_bootstrap_servers.split(","),
                    client_id="measurement-session-catalog-api",
                    group_id=self._settings.kafka_consumer_group,
                    enable_auto_commit=False,
                    auto_offset_reset="earliest",
                    request_timeout_ms=30_000,
                    api_version_auto_timeout_ms=10_000,
                )
                self.failure = None
                self._ready.set()
                while not self._stop.is_set():
                    records = consumer.poll(timeout_ms=1_000)
                    for partition_records in records.values():
                        for record in partition_records:
                            self.process_record(record)
                            consumer.commit()
            except (KafkaError, OSError, psycopg.Error) as error:
                self._ready.clear()
                self.failure = f"{type(error).__name__}: {error}"
                LOGGER.warning(
                    "MeasurementSession catalog materialization is unavailable; retrying: %s",
                    self.failure,
                )
                self._stop.wait(self._retry_interval_seconds)
            except (CatalogError, ContractValidationError, DecodeError, ValueError) as error:
                self._ready.clear()
                self.failure = f"{type(error).__name__}: {error}"
                LOGGER.exception("MeasurementSession catalog materialization stopped")
                return
            finally:
                if consumer is not None:
                    consumer.close(autocommit=False)