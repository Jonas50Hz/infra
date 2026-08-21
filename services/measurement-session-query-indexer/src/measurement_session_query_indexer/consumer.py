"""Manual-commit Blobmeta consumer that registers verified Iceberg files."""

from __future__ import annotations

from collections.abc import Callable
import logging
from pathlib import Path
from threading import Event
from typing import Any, Protocol

from google.protobuf.message import DecodeError
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import psycopg

from measurement_session_common.contract import (
    ContractValidationError,
    validate_blobmeta,
    validate_kafka_key,
)
from measurement_session_common.generated.blobmeta_pb2 import Blobmeta
from measurement_session_query_indexer.artifact import ArtifactVerificationError, VerifiedArtifact
from measurement_session_query_indexer.config import Settings
from measurement_session_query_indexer.ledger import LedgerError
from measurement_session_query_indexer.trino import TrinoConnectionError, TrinoStatementError

LOGGER = logging.getLogger(__name__)


class ArtifactVerifier(Protocol):
    """Verify one Blobmeta artifact before external registration."""

    def __call__(self, result: Blobmeta) -> VerifiedArtifact:
        """Return immutable artifact evidence or raise a verification error."""


class RegistrationLedger(Protocol):
    """Durably record one verified artifact after Iceberg reconciliation."""

    def register(self, artifact: VerifiedArtifact, callback: Callable[[VerifiedArtifact], object]) -> bool:
        """Run callback under a blob-scoped registration lock."""


class SessionWriter(Protocol):
    """Reconcile one exact file in the internal Iceberg table."""

    def ensure_registered(self, artifact: VerifiedArtifact) -> object:
        """Register or validate the canonical object URI."""


class BlobmetaQueryIndexer:
    """Commit a Kafka offset only after a Blobmeta artifact is queryable."""

    def __init__(
        self,
        settings: Settings,
        verifier: ArtifactVerifier,
        ledger: RegistrationLedger,
        writer: SessionWriter,
        consumer_factory: Callable[..., Any] = KafkaConsumer,
    ) -> None:
        self._settings = settings
        self._verifier = verifier
        self._ledger = ledger
        self._writer = writer
        self._consumer_factory = consumer_factory
        self._stop = Event()
        self._ready_path = Path("/tmp/wama-query-indexer-ready")

    def run(self) -> None:
        """Reconnect transient infrastructure failures without advancing offsets."""

        self._clear_ready()
        while not self._stop.is_set():
            consumer: Any | None = None
            try:
                consumer = self._consumer_factory(
                    self._settings.kafka_topic,
                    bootstrap_servers=self._settings.kafka_bootstrap_servers.split(","),
                    client_id="measurement-session-query-indexer",
                    group_id=self._settings.kafka_consumer_group,
                    enable_auto_commit=False,
                    auto_offset_reset=self._settings.kafka_auto_offset_reset,
                    request_timeout_ms=30_000,
                    api_version_auto_timeout_ms=10_000,
                )
                while not self._stop.is_set():
                    records = consumer.poll(timeout_ms=1_000)
                    if consumer.assignment():
                        self._ready_path.touch(exist_ok=True)
                    for partition_records in records.values():
                        for record in partition_records:
                            self.process_record(record)
                            consumer.commit()
            except (KafkaError, OSError, psycopg.OperationalError, TrinoConnectionError) as error:
                self._clear_ready()
                LOGGER.warning("Measurement-session query indexer will retry: %s", error)
                self._stop.wait(self._settings.kafka_retry_interval_seconds)
            except (
                ArtifactVerificationError,
                ContractValidationError,
                DecodeError,
                LedgerError,
                TrinoStatementError,
                ValueError,
            ):
                LOGGER.exception("Measurement-session query indexer stopped on immutable evidence")
                raise
            finally:
                if consumer is not None:
                    consumer.close(autocommit=False)

    def _clear_ready(self) -> None:
        self._ready_path.unlink(missing_ok=True)

    def process_record(self, record: Any) -> bool:
        """Validate one Blobmeta result and make successful artifacts queryable."""

        result = Blobmeta()
        try:
            result.ParseFromString(record.value)
        except DecodeError as error:
            raise ArtifactVerificationError("Blobmeta payload is not valid raw Protobuf") from error
        validate_blobmeta(result)
        validate_kafka_key(record.key, result.blob_id, "blob_id")
        if result.status == Blobmeta.REJECTED:
            return False
        artifact = self._verifier(result)
        return self._ledger.register(artifact, self._writer.ensure_registered)