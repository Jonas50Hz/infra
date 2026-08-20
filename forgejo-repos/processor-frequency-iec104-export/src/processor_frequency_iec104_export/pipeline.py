"""Quixstreams pipeline from raw Common Format frequency to raw ExportRecord."""

from __future__ import annotations

import logging
from typing import Any

from quixstreams import Application
from quixstreams.models.serializers.protobuf import ProtobufDeserializer, ProtobufSerializer

from processor_frequency_iec104_export.config import Settings
from processor_frequency_iec104_export.export import ExportEnvelope, build_export
from processor_frequency_iec104_export.generated import iec104_export_pb2, rtd_schema_pb2


LOGGER = logging.getLogger(__name__)


def build_transformation_stream(application: Application, settings: Settings) -> Any:
    """Build the raw-Protobuf input filter and typed IEC export transformation."""

    input_topic = application.topic(
        settings.input_topic,
        key_deserializer="bytes",
        value_deserializer=ProtobufDeserializer(
            msg_type=rtd_schema_pb2.MCCSMeasurementValue,
            to_dict=False,
        ),
    )
    stream = application.dataframe(input_topic)
    stream = stream.apply(
        lambda source, key, timestamp, _headers: build_export(source, key, timestamp, settings),
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
    """Start the direct configured frequency export pipeline."""

    settings = Settings.from_environment()
    LOGGER.info(
        "Starting direct frequency IEC 104 export group %s from %s to %s",
        settings.consumer_group,
        settings.input_topic,
        settings.output_topic,
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