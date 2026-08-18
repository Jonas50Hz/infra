"""Quixstreams entry point for the WAMA frequency-scale processor."""

from __future__ import annotations

import logging
import os
from typing import Any

from quixstreams import Application
from quixstreams.models.serializers.protobuf import ProtobufDeserializer, ProtobufSerializer

from processor_frequency_scale.config import ConfigurationError, ProcessorConfig, load_config
from processor_frequency_scale.generated.rtd_schema_pb2 import MCCSMeasurementValue
from processor_frequency_scale.pipeline import (
    output_key,
    preserve_kafka_timestamp,
    transform,
)

LOGGER = logging.getLogger(__name__)


def build_transformation_stream(application: Application, config: ProcessorConfig) -> Any:
    """Build the source-filtering transformation before its output topic is attached."""

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
        lambda measurement, key, timestamp, _headers: transform(
            measurement,
            key,
            timestamp,
        ),
        metadata=True,
    )
    stream = stream.filter(lambda derived: derived is not None)
    stream = stream.set_timestamp(preserve_kafka_timestamp)
    return stream.apply(lambda derived: derived.measurement)


def build_output_stream(application: Application, config: ProcessorConfig) -> Any:
    """Attach the output topic while retaining the derived Kafka timestamp."""

    output_topic = application.topic(
        config.output_topic,
        key_serializer="bytes",
        value_serializer=ProtobufSerializer(msg_type=MCCSMeasurementValue),
    )
    stream = build_transformation_stream(application, config)
    return stream.to_topic(output_topic, key=output_key)


def build_application(config: ProcessorConfig) -> tuple[Application, Any]:
    """Build a raw-Protobuf pipeline without creating Kafka topics."""

    application = Application(
        broker_address=config.kafka_bootstrap_servers,
        consumer_group=config.consumer_group,
        auto_create_topics=False,
        auto_offset_reset="latest",
        processing_guarantee="at-least-once",
    )
    stream = build_output_stream(application, config)
    return application, stream


def run_application(config: ProcessorConfig) -> None:
    """Run the configured output stream through Quixstreams."""

    application, stream = build_application(config)
    application.run(stream)


def main() -> None:
    """Load processor configuration and run its pipeline."""

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config_path = os.environ.get("PROCESSOR_CONFIG_PATH", "/etc/wama/processor.yaml")
    try:
        config = load_config(config_path)
    except ConfigurationError as error:
        LOGGER.error("Invalid processor startup configuration: %s", error)
        raise SystemExit(2) from error

    LOGGER.info(
        "Starting processor group %s from %s to %s",
        config.consumer_group,
        config.input_topic,
        config.output_topic,
    )
    run_application(config)


if __name__ == "__main__":
    main()