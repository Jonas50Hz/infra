"""Clocked Kafka adapter for per-second LFR preferred-frequency provision."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import logging
import os
import time
from typing import Any

from quixstreams import Application
from quixstreams.models.serializers.protobuf import ProtobufDeserializer, ProtobufSerializer

from processor_lfr_frequency_provision.codec import incoming_measurement, preferred_measurement
from processor_lfr_frequency_provision.config import LfrConfig, load_config
from processor_lfr_frequency_provision.engine import LfrSecondEngine
from processor_lfr_frequency_provision.generated import rtd_schema_pb2
from processor_lfr_frequency_provision.state import PendingPublication, StateStore


LOGGER = logging.getLogger(__name__)


class RuntimeConfigurationError(ValueError):
    """Raised when an LFR runtime environment override is unusable."""


@dataclass(frozen=True)
class RuntimeSettings:
    """Runtime paths and Kafka connection settings for the standalone processor."""

    config_path: str
    consumer_group: str
    input_topic: str
    kafka_bootstrap_servers: str
    output_topic: str
    poll_interval_ms: int
    state_path: str

    @classmethod
    def from_environment(cls) -> RuntimeSettings:
        """Load safe application-local runtime values from the environment."""

        return cls(
            config_path=_required("LFR_CONFIG_PATH", "/etc/wama/lfr-config.yaml"),
            consumer_group=_required(
                "WAMA_PROCESSOR_CONSUMER_GROUP",
                "processor-lfr-frequency-provision",
            ),
            input_topic=_required("WAMA_INPUT_TOPIC", "LiveMeasurement"),
            kafka_bootstrap_servers=_required("WAMA_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            output_topic=_required("WAMA_OUTPUT_TOPIC", "LiveMeasurement"),
            poll_interval_ms=_integer("LFR_POLL_INTERVAL_MS", 50, 10, 100),
            state_path=_required("LFR_STATE_PATH", "/var/lib/wama/lfr-state.sqlite3"),
        )


def build_application(settings: RuntimeSettings) -> tuple[Application, Any, Any]:
    """Build topics for the manual consumer loop without auto-creating Kafka topics."""

    application = Application(
        broker_address=settings.kafka_bootstrap_servers,
        consumer_group=settings.consumer_group,
        auto_create_topics=False,
        auto_offset_reset="latest",
        processing_guarantee="at-least-once",
    )
    input_topic = application.topic(
        settings.input_topic,
        key_deserializer="bytes",
        value_deserializer=ProtobufDeserializer(
            msg_type=rtd_schema_pb2.MCCSMeasurementValue,
            to_dict=False,
        ),
    )
    output_topic = application.topic(
        settings.output_topic,
        key_serializer="bytes",
        value_serializer=ProtobufSerializer(msg_type=rtd_schema_pb2.MCCSMeasurementValue),
    )
    return application, input_topic, output_topic


def run_processor() -> None:
    """Run the deadline-driven LFR processor with durable state and outbox retries."""

    settings = RuntimeSettings.from_environment()
    config = load_config(settings.config_path)
    state_store = StateStore(settings.state_path)
    engine = LfrSecondEngine(config, state_store.load_snapshot())
    application, input_topic, output_topic = build_application(settings)
    consumer = application.get_consumer(auto_commit_enable=False)
    producer = application.get_producer()
    LOGGER.warning(
        "LFR status evidence mode %s is provisional until source PMU status mapping is finalized",
        config.status_evidence_mode,
    )
    try:
        run_loop(
            config=config,
            consumer=consumer,
            engine=engine,
            input_topic=input_topic,
            output_topic=output_topic,
            poll_interval_ms=settings.poll_interval_ms,
            producer=producer,
            state_store=state_store,
        )
    finally:
        state_store.close()


def run_loop(
    *,
    config: LfrConfig,
    consumer: Any,
    engine: LfrSecondEngine,
    input_topic: Any,
    output_topic: Any,
    poll_interval_ms: int,
    producer: Any,
    state_store: StateStore,
    now_ms: Callable[[], int] | None = None,
    keep_running: Callable[[], bool] | None = None,
) -> None:
    """Consume records and close seconds at processing-time deadlines even when idle."""

    clock = _now_ms if now_ms is None else now_ms
    running = (lambda: True) if keep_running is None else keep_running
    consumer.subscribe([input_topic.name])
    try:
        while running():
            current_time_ms = clock()
            _close_and_persist(config, engine, state_store, current_time_ms)
            _publish_pending(output_topic, producer, state_store)

            timeout_seconds = _poll_timeout_seconds(
                current_time_ms,
                config.close_delay_ms,
                poll_interval_ms,
            )
            raw_message = consumer.poll(timeout=timeout_seconds)
            if raw_message is None:
                continue
            if raw_message.error() is not None:
                LOGGER.warning("Kafka consumer event while waiting for LFR input: %s", raw_message.error())
                continue

            decoded = input_topic.deserialize(raw_message)
            source = incoming_measurement(
                decoded.value,
                _input_id(raw_message),
            )
            result = engine.ingest(source, clock())
            if result.reasons:
                LOGGER.info(
                    "LFR rejected %s with reasons %s",
                    source.input_id,
                    ",".join(reason.value for reason in result.reasons),
                )
            state_store.persist(engine.snapshot())
            consumer.commit(raw_message, asynchronous=False)
    finally:
        producer.flush(timeout=10)
        consumer.close()


def _close_and_persist(
    config: LfrConfig,
    engine: LfrSecondEngine,
    state_store: StateStore,
    current_time_ms: int,
) -> tuple[PendingPublication, ...]:
    closed_seconds = engine.close_ready(current_time_ms)
    publications = tuple(
        publication
        for closed in closed_seconds
        if (publication := PendingPublication.from_closed_second(closed, config.output_mrid))
        is not None
    )
    if closed_seconds:
        state_store.persist(engine.snapshot(), publications)
    return publications


def _publish_pending(output_topic: Any, producer: Any, state_store: StateStore) -> None:
    for publication in state_store.pending_publications():
        serialized = output_topic.serialize(
            key=publication.output_mrid.encode("utf-8"),
            value=preferred_measurement(publication),
            timestamp_ms=publication.closed_at_ms,
        )
        delivery_errors: list[object] = []

        def on_delivery(error: object, _message: object) -> None:
            if error is not None:
                delivery_errors.append(error)

        producer.produce(
            output_topic.name,
            value=serialized.value,
            key=serialized.key,
            headers=serialized.headers,
            timestamp=serialized.timestamp,
            on_delivery=on_delivery,
        )
        outstanding = producer.flush(timeout=10)
        if outstanding:
            raise RuntimeError(
                f"LFR Kafka output did not flush publication {publication.publication_id}"
            )
        if delivery_errors:
            raise RuntimeError(
                f"LFR Kafka output failed for publication {publication.publication_id}: "
                f"{delivery_errors[0]}"
            )
        state_store.mark_delivered(publication.publication_id)


def _poll_timeout_seconds(
    current_time_ms: int,
    close_delay_ms: int,
    poll_interval_ms: int,
) -> float:
    second_start_ms = (current_time_ms // 1_000) * 1_000
    current_close_deadline_ms = second_start_ms + close_delay_ms
    next_deadline_ms = (
        current_close_deadline_ms
        if current_time_ms < current_close_deadline_ms
        else second_start_ms + 1_000 + close_delay_ms
    )
    remaining_ms = max(0, next_deadline_ms - current_time_ms)
    return min(poll_interval_ms, remaining_ms) / 1_000


def _input_id(raw_message: Any) -> str:
    return f"{raw_message.topic()}:{raw_message.partition()}:{raw_message.offset()}"


def _now_ms() -> int:
    return time.time_ns() // 1_000_000


def _required(name: str, default: str) -> str:
    value = os.environ.get(name, default).strip()
    if not value:
        raise RuntimeConfigurationError(f"{name} must not be empty")
    return value


def _integer(name: str, default: int, minimum: int, maximum: int) -> int:
    raw_value = os.environ.get(name, str(default))
    try:
        value = int(raw_value)
    except ValueError as error:
        raise RuntimeConfigurationError(f"{name} must be an integer") from error
    if not minimum <= value <= maximum:
        raise RuntimeConfigurationError(f"{name} must be between {minimum} and {maximum}")
    return value