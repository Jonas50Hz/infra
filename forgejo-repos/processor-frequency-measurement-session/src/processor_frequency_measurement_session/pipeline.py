"""Quixstreams wiring from raw LiveMeasurement to raw MeasurementSession."""

from __future__ import annotations

import logging
from typing import Any

from quixstreams import Application
from quixstreams.models.serializers.protobuf import ProtobufDeserializer, ProtobufSerializer

from processor_frequency_measurement_session.capture import (
    EpisodeTracker,
    SessionRequestEnvelope,
    request_key,
)
from processor_frequency_measurement_session.config import Settings
from processor_frequency_measurement_session.generated import (
    measurement_session_pb2,
    rtd_schema_pb2,
)
from processor_frequency_measurement_session.policy import CAPTURE_POLICIES


LOGGER = logging.getLogger(__name__)


def build_transformation_stream(
    application: Application,
    settings: Settings,
    tracker: EpisodeTracker | None = None,
) -> Any:
    """Build one raw-Protobuf event-time capture transformation stream."""

    episode_tracker = EpisodeTracker() if tracker is None else tracker
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
        lambda source, key, _timestamp, _headers: episode_tracker.transform(source, key),
        metadata=True,
    )
    stream = stream.filter(lambda envelope: envelope is not None)
    stream = stream.set_timestamp(_request_timestamp)
    return stream.apply(lambda envelope: envelope.request)


def build_output_stream(
    application: Application,
    settings: Settings,
    tracker: EpisodeTracker | None = None,
) -> Any:
    """Publish raw canonical session requests without creating Kafka topics."""

    output_topic = application.topic(
        settings.output_topic,
        key_serializer="bytes",
        value_serializer=ProtobufSerializer(
            msg_type=measurement_session_pb2.MeasurementSessionRequest,
        ),
    )
    return build_transformation_stream(application, settings, tracker).to_topic(
        output_topic,
        key=request_key,
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
    """Start the direct reviewed frequency-session capture pipeline."""

    settings = Settings.from_environment()
    LOGGER.info(
        "Starting frequency measurement-session capture group %s from %s to %s with %s policies",
        settings.consumer_group,
        settings.input_topic,
        settings.output_topic,
        len(CAPTURE_POLICIES),
    )
    application, stream = build_application(settings)
    application.run(stream)


def _request_timestamp(
    envelope: SessionRequestEnvelope,
    _key: bytes | None,
    _timestamp: int,
    _headers: object,
) -> int:
    return envelope.kafka_timestamp_ms