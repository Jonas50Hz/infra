"""At-least-once Kafka worker that materializes session artifacts exactly once."""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from threading import Event
from typing import Any, Callable

from google.protobuf.message import DecodeError
from google.protobuf.timestamp_pb2 import Timestamp
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from measurement_session_common.contract import (
    BLOBMETA_MEDIA_TYPE,
    DEFAULT_SESSION_BUCKET,
    PARQUET_MEDIA_TYPE,
    ContractValidationError,
    rejected_blob_id,
    rejected_receipt_key,
    request_sha256,
    session_parquet_key,
    successful_blob_id,
    successful_receipt_key,
    validate_blobmeta,
    validate_kafka_key,
    validate_measurement_session_request,
)
from measurement_session_common.generated.blobmeta_pb2 import Blobmeta
from measurement_session_common.generated.measurement_session_pb2 import MeasurementSessionRequest
from measurement_session_processor.config import Settings
from measurement_session_processor.druid import DruidClient, DruidQueryError
from measurement_session_processor.parquet import ArtifactStats, SessionArtifactError, write_session_parquet
from measurement_session_processor.storage import ObjectStoreError, SeaweedSessionStore

LOGGER = logging.getLogger(__name__)


class SessionProcessingError(RuntimeError):
    """Raised for malformed input that must remain visible to operators."""


class SessionWorker:
    """Consume bounded requests and publish one deterministic Blobmeta result."""

    def __init__(
        self,
        settings: Settings,
        druid: DruidClient,
        storage: SeaweedSessionStore,
        producer: Any,
        clock: Callable[[], datetime] | None = None,
        consumer_factory: Callable[..., Any] = KafkaConsumer,
    ) -> None:
        self._settings = settings
        self._druid = druid
        self._storage = storage
        self._producer = producer
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._consumer_factory = consumer_factory
        self._stop = Event()

    def run(self) -> None:
        """Run a reconnecting manual-commit consumer until the container stops."""

        while not self._stop.is_set():
            consumer: Any | None = None
            try:
                consumer = self._consumer_factory(
                    self._settings.kafka_topic,
                    bootstrap_servers=self._settings.kafka_bootstrap_servers.split(","),
                    client_id="measurement-session-processor",
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
            except (KafkaError, OSError, ObjectStoreError, DruidQueryError, SessionArtifactError) as error:
                LOGGER.warning("MeasurementSession worker will retry: %s", error)
                self._stop.wait(self._settings.kafka_retry_interval_seconds)
            except SessionProcessingError:
                LOGGER.exception("MeasurementSession worker stopped on malformed input")
                raise
            finally:
                if consumer is not None:
                    consumer.close(autocommit=False)

    def process_record(self, record: Any) -> Blobmeta:
        """Materialize one Kafka record; callers commit only after this returns."""

        request = MeasurementSessionRequest()
        try:
            request.ParseFromString(record.value)
        except DecodeError as error:
            raise SessionProcessingError("MeasurementSession payload is not raw Protobuf") from error
        try:
            validate_kafka_key(record.key, request.session_id, "session_id")
        except ContractValidationError as error:
            raise SessionProcessingError(str(error)) from error

        digest = request_sha256(request)
        try:
            validate_measurement_session_request(
                request,
                max_mrids=self._settings.max_mrids,
                max_interval_hours=self._settings.max_interval_hours,
            )
        except ContractValidationError as error:
            return self._reject_request(request, digest, error)
        return self._materialize_request(request, digest)

    def _materialize_request(
        self,
        request: MeasurementSessionRequest,
        digest: bytes,
    ) -> Blobmeta:
        receipt_key = successful_receipt_key(request.session_id)
        existing = self._existing_result(receipt_key, request, digest)
        if existing is not None:
            return self._publish(existing)

        with TemporaryDirectory(prefix="wama-measurement-session-") as temporary_directory:
            artifact_path = Path(temporary_directory) / "measurements.parquet"
            stats = write_session_parquet(
                artifact_path,
                self._druid.iter_rows(request),
                tuple(request.mrids),
                self._settings.max_rows,
                self._settings.parquet_batch_rows,
                self._settings.max_artifact_bytes,
            )
            result = self._completed_result(request, digest, stats)
            validate_blobmeta(result)
            payload = result.SerializeToString(deterministic=True)
            self._storage.put_or_verify_file(
                self._settings.s3_bucket,
                result.object.object_key,
                artifact_path,
                result.object.media_type,
                bytes(result.object.sha256),
                result.object.byte_length,
            )
            self._storage.put_or_verify_bytes(
                self._settings.s3_bucket,
                receipt_key,
                payload,
                BLOBMETA_MEDIA_TYPE,
            )
        return self._publish(result)

    def _reject_request(
        self,
        request: MeasurementSessionRequest,
        digest: bytes,
        error: ContractValidationError,
    ) -> Blobmeta:
        try:
            blob_id = rejected_blob_id(request.session_id, digest)
            receipt_key = rejected_receipt_key(request.session_id, digest)
        except ContractValidationError as identity_error:
            raise SessionProcessingError(str(identity_error)) from identity_error
        existing = self._existing_result(receipt_key, request, digest)
        if existing is not None:
            return self._publish(existing)

        result = Blobmeta(
            blob_id=blob_id,
            session_id=request.session_id,
            request_sha256=digest,
            status=Blobmeta.REJECTED,
            rejection_reason=_bounded_reason(str(error)),
        )
        self._copy_timestamps(result, request)
        _timestamp_from_datetime(result.finalized_at, self._clock())
        try:
            validate_blobmeta(result)
        except ContractValidationError as rejection_error:
            raise SessionProcessingError(
                f"MeasurementSession cannot produce an auditable rejection: {rejection_error}"
            ) from rejection_error
        payload = result.SerializeToString(deterministic=True)
        self._storage.put_or_verify_bytes(
            self._settings.s3_bucket,
            receipt_key,
            payload,
            BLOBMETA_MEDIA_TYPE,
        )
        return self._publish(result)

    def _existing_result(
        self,
        receipt_key: str,
        request: MeasurementSessionRequest,
        digest: bytes,
    ) -> Blobmeta | None:
        payload = self._storage.read_receipt(self._settings.s3_bucket, receipt_key)
        if payload is None:
            return None
        result = Blobmeta()
        try:
            result.ParseFromString(payload)
            validate_blobmeta(result)
        except (DecodeError, ContractValidationError) as error:
            raise SessionProcessingError("Blobmeta replay receipt is malformed") from error
        if result.session_id != request.session_id or bytes(result.request_sha256) != digest:
            raise SessionProcessingError("Blobmeta replay receipt does not match MeasurementSession request")
        if result.HasField("object"):
            self._storage.verify_object(
                result.object.bucket,
                result.object.object_key,
                result.object.media_type,
                bytes(result.object.sha256),
                result.object.byte_length,
            )
        return result

    def _completed_result(
        self,
        request: MeasurementSessionRequest,
        digest: bytes,
        stats: ArtifactStats,
    ) -> Blobmeta:
        result = Blobmeta(
            blob_id=successful_blob_id(request.session_id),
            session_id=request.session_id,
            request_sha256=digest,
            mrids=request.mrids,
            measurement_count=stats.measurement_count,
            status=(
                Blobmeta.COMPLETE
                if all(count > 0 for _, count in stats.coverage)
                else Blobmeta.PARTIAL
            ),
        )
        self._copy_timestamps(result, request)
        _timestamp_from_datetime(result.finalized_at, self._clock())
        for metadata in request.metadata:
            result.metadata.add().CopyFrom(metadata)
        for mrid, count in stats.coverage:
            coverage = result.mrid_coverage.add()
            coverage.mrid = mrid
            coverage.measurement_count = count
        result.object.bucket = DEFAULT_SESSION_BUCKET
        result.object.object_key = session_parquet_key(request.session_id)
        result.object.media_type = PARQUET_MEDIA_TYPE
        result.object.byte_length = stats.size_bytes
        result.object.sha256 = stats.sha256
        return result

    def _copy_timestamps(
        self,
        result: Blobmeta,
        request: MeasurementSessionRequest,
    ) -> None:
        result.requested_at.CopyFrom(request.requested_at)
        result.started_at.CopyFrom(request.started_at)
        result.ended_at.CopyFrom(request.ended_at)

    def _publish(self, result: Blobmeta) -> Blobmeta:
        payload = result.SerializeToString(deterministic=True)
        timestamp_milliseconds = (
            result.finalized_at.seconds * 1_000 + result.finalized_at.nanos // 1_000_000
        )
        self._producer.send(
            self._settings.blobmeta_topic,
            key=result.blob_id.encode("utf-8"),
            value=payload,
            timestamp_ms=timestamp_milliseconds,
        ).get(timeout=30)
        return result


def _timestamp_from_datetime(destination: Timestamp, value: datetime) -> None:
    destination.FromDatetime(value.astimezone(timezone.utc))


def _bounded_reason(value: str) -> str:
    encoded = value.encode("utf-8")[:1024]
    return encoded.decode("utf-8", errors="ignore") or "MeasurementSession request was rejected"