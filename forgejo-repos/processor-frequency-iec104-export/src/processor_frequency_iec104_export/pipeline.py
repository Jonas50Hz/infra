"""Quixstreams pipeline from raw Common Format frequency to raw ExportRecord."""

from __future__ import annotations

import logging
from typing import Any

from quixstreams import Application
from quixstreams.models.serializers.protobuf import ProtobufDeserializer, ProtobufSerializer

from processor_frequency_iec104_export.config import Settings
from processor_frequency_iec104_export.export import ExportEnvelope, FrequencySecondAggregator
from processor_frequency_iec104_export.generated import iec104_export_pb2, rtd_schema_pb2


LOGGER = logging.getLogger(__name__)


def build_transformation_stream(application: Application, settings: Settings) -> Any:
    """Build the raw-Protobuf event-time aggregation and IEC export transformation."""

    input_topic = application.topic(
        settings.input_topic,
        key_deserializer="bytes",
        value_deserializer=ProtobufDeserializer(
            msg_type=rtd_schema_pb2.MCCSMeasurementValue,
            to_dict=False,
        ),
    )
    stream = application.dataframe(input_topic)
    aggregator = FrequencySecondAggregator(settings)
    stream = stream.apply(
        lambda source, key, _timestamp, _headers: aggregator.process(source, key),
        metadata=True,
    )
    stream = stream.filter(lambda envelope: envelope is not None)
    stream = stream.set_timestamp(_export_timestamp)
    return stream.apply(lambda envelope: envelope.record)


def build_output_stream(application: Application, settings: Settings) -> Any:
    """Publish raw ExportRecord values to the provisioned Export topic."""

    output_topic = application.topic(
        settings.output_topic,
        key_serializer="bytes",
        value_serializer=ProtobufSerializer(msg_type=iec104_export_pb2.ExportRecord),
    )
    return build_transformation_stream(application, settings).to_topic(
        output_topic,
        key=lambda record: record.export_id.encode("utf-8"),
    )


def build_application(settings: Settings | None = None) -> tuple[Application, Any]:
    """Build an at-least-once processor without creating Kafka topics."""

    runtime_settings = settings or Settings.from_environment()
    application = Application(
        broker_address=runtime_settings.kafka_bootstrap_servers,
        consumer_group=runtime_settings.consumer_group,
        auto_create_topics=False,
        auto_offset_reset="latest",
        processing_guarantee="at-least-once",
    )
    return application, build_output_stream(application, runtime_settings)


def run_processor() -> None:
    """Start the configured per-event-second frequency export pipeline."""

    settings = Settings.from_environment()
    LOGGER.info(
        "Starting per-event-second frequency IEC 104 export group %s from %s to %s with %s mappings",
        settings.consumer_group,
        settings.input_topic,
        settings.output_topic,
        len(settings.mappings),
    )
    application, stream = build_application(settings)
    application.run(stream)


def _export_timestamp(
    envelope: ExportEnvelope,
    _key: bytes | None,
    _timestamp: int,
    _headers: object,
) -> int:
    return envelope.kafka_timestamp_ms