"""Framework-owned Quixstreams runtime for WAMA processor definitions."""

from __future__ import annotations

from dataclasses import dataclass
import logging
import os
from typing import Any

from quixstreams import Application
from quixstreams.models.serializers.protobuf import ProtobufDeserializer, ProtobufSerializer

from wama_processor.definition import DerivedMeasurement, ProcessorDefinition
from wama_processor.generated.rtd_schema_pb2 import MCCSMeasurementValue

LOGGER = logging.getLogger(__name__)


class RuntimeConfigurationError(ValueError):
    """Raised when an advanced runtime override is invalid."""


@dataclass(frozen=True)
class RuntimeConfig:
    """Kafka settings with safe WAMA defaults for normal processor authoring."""

    kafka_bootstrap_servers: str
    consumer_group: str
    input_topic: str
    output_topic: str

    @classmethod
    def from_environment(cls, service_name: str) -> RuntimeConfig:
        """Load optional runtime overrides without a processor-specific YAML file."""

        return cls(
            kafka_bootstrap_servers=_environment_value(
                "WAMA_KAFKA_BOOTSTRAP_SERVERS",
                "kafka:9092",
            ),
            consumer_group=_environment_value(
                "WAMA_PROCESSOR_CONSUMER_GROUP",
                service_name,
            ),
            input_topic=_environment_value("WAMA_INPUT_TOPIC", "LiveMeasurement"),
            output_topic=_environment_value("WAMA_OUTPUT_TOPIC", "LiveMeasurement"),
        )


def build_transformation_stream(
    application: Application,
    definition: ProcessorDefinition,
    config: RuntimeConfig,
) -> Any:
    """Build the framework-owned input filter and transformation stream."""

    input_topic = application.topic(
        config.input_topic,
        key_deserializer="bytes",
        value_deserializer=ProtobufDeserializer(
            msg_type=MCCSMeasurementValue,
            to_dict=False,
        ),
    )
    stream = application.dataframe(input_topic)
    stream = stream.apply(
        lambda measurement, key, timestamp, _headers: definition.transform_record(
            measurement,
            key,
            timestamp,
        ),
        metadata=True,
    )
    stream = stream.filter(lambda derived: derived is not None)
    stream = stream.set_timestamp(_source_kafka_timestamp)
    return stream.apply(lambda derived: derived.protobuf)


def build_output_stream(
    application: Application,
    definition: ProcessorDefinition,
    config: RuntimeConfig,
) -> Any:
    """Attach the raw-Protobuf output topic to the common transformation stream."""

    output_topic = application.topic(
        config.output_topic,
        key_serializer="bytes",
        value_serializer=ProtobufSerializer(msg_type=MCCSMeasurementValue),
    )
    stream = build_transformation_stream(application, definition, config)
    return stream.to_topic(
        output_topic,
        key=lambda measurement: measurement.mrid.encode("utf-8"),
    )


def build_application(
    definition: ProcessorDefinition,
    config: RuntimeConfig | None = None,
) -> tuple[Application, Any]:
    """Build an at-least-once Common Format processor without creating topics."""

    runtime_config = config or RuntimeConfig.from_environment(definition.service_name)
    application = Application(
        broker_address=runtime_config.kafka_bootstrap_servers,
        consumer_group=runtime_config.consumer_group,
        auto_create_topics=False,
        auto_offset_reset="latest",
        processing_guarantee="at-least-once",
    )
    stream = build_output_stream(application, definition, runtime_config)
    return application, stream


def run_processor(definition: ProcessorDefinition) -> None:
    """Start a declared processor with its safe default runtime settings."""

    config = RuntimeConfig.from_environment(definition.service_name)
    LOGGER.info(
        "Starting processor group %s from %s to %s",
        config.consumer_group,
        config.input_topic,
        config.output_topic,
    )
    application, stream = build_application(definition, config)
    application.run(stream)


def _environment_value(name: str, default: str) -> str:
    value = os.environ.get(name, default)
    if not value.strip():
        raise RuntimeConfigurationError(f"{name} must be a non-empty string")
    return value.strip()


def _source_kafka_timestamp(
    derived: DerivedMeasurement,
    _key: bytes | None,
    _timestamp: int,
    _headers: object,
) -> int:
    return derived.kafka_timestamp_ms
