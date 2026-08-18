"""Quixstreams entry point for an application-owned WAMA processor."""

from __future__ import annotations

import logging
import os
from typing import Any

from quixstreams import Application
from quixstreams.models.serializers.protobuf import ProtobufDeserializer, ProtobufSerializer

from processor_template.config import ConfigurationError, ProcessorConfig, load_config
from processor_template.generated.rtd_schema_pb2 import MCCSMeasurementValue
from processor_template.pipeline import transform

LOGGER = logging.getLogger(__name__)


def build_application(config: ProcessorConfig) -> tuple[Application, Any]:
    """Build a raw-Protobuf application pipeline without creating Kafka topics."""

    application = Application(
        broker_address=config.kafka_bootstrap_servers,
        consumer_group=config.consumer_group,
        auto_create_topics=False,
        auto_offset_reset="latest",
        processing_guarantee="at-least-once",
    )
    input_topic = application.topic(
        config.input_topic,
        key_deserializer="bytes",
        value_deserializer=ProtobufDeserializer(
            msg_type=MCCSMeasurementValue,
            to_dict=False,
        ),
    )
    output_topic = application.topic(
        config.output_topic,
        key_serializer="bytes",
        value_serializer=ProtobufSerializer(msg_type=MCCSMeasurementValue),
    )
    stream = application.dataframe(input_topic)
    stream = stream.apply(transform)
    stream = stream.filter(lambda measurement: measurement is not None)
    stream = stream.to_topic(
        output_topic,
        key=lambda measurement: measurement.mrid.encode("utf-8"),
    )
    return application, stream


def run_application(config: ProcessorConfig) -> None:
    """Run the configured output stream through Quixstreams."""

    application, stream = build_application(config)
    application.run(stream)


def main() -> None:
    """Load application processor configuration and run its pipeline."""

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
        "Starting application processor group %s from %s to %s",
        config.consumer_group,
        config.input_topic,
        config.output_topic,
    )
    run_application(config)


if __name__ == "__main__":
    main()