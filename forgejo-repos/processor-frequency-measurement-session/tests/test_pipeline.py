"""Quixstreams pipeline wiring tests without Kafka or private producer access."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from quixstreams import Application
from quixstreams.models.messagecontext import MessageContext

from processor_frequency_measurement_session.capture import EpisodeTracker
from processor_frequency_measurement_session.config import Settings
from processor_frequency_measurement_session.generated import (
    measurement_session_pb2,
    rtd_schema_pb2,
)
from processor_frequency_measurement_session.pipeline import (
    build_application,
    build_output_stream,
    build_transformation_stream,
)
from processor_frequency_measurement_session.policy import CAPTURE_POLICIES


FREQUENCY_MRID = CAPTURE_POLICIES[0].frequency_mrid


class PipelineTests(unittest.TestCase):
    """Require raw-Protobuf topic wiring and event-time output timestamps."""

    def test_transformation_uses_timestamp_mccs_not_input_kafka_timestamp(self) -> None:
        application = self._application()
        tracker = EpisodeTracker(sleeper=lambda _seconds: None)
        tracker.transform(_frequency(50.3, seconds=1_726_000_100), FREQUENCY_MRID.encode())
        source = _frequency(50.0, seconds=1_726_000_101, nanos=987_654_321)

        result = build_transformation_stream(application, Settings.from_environment({}), tracker).test(
            source,
            key=source.mrid.encode("utf-8"),
            timestamp=1_111,
            headers=[("trace-id", b"frequency-session")],
        )

        self.assertEqual(len(result), 1)
        request, key, timestamp, headers = result[0]
        self.assertEqual(key, source.mrid.encode("utf-8"))
        self.assertEqual(timestamp, 1_726_000_101_987)
        self.assertEqual(headers, [("trace-id", b"frequency-session")])
        self.assertEqual(request.requested_at.ToMilliseconds(), timestamp)

    def test_output_serializes_request_and_uses_session_id_key(self) -> None:
        application = self._application()
        tracker = EpisodeTracker(sleeper=lambda _seconds: None)
        tracker.transform(_frequency(50.3, seconds=1_726_000_100), FREQUENCY_MRID.encode())
        source = _frequency(50.0, seconds=1_726_000_101, nanos=987_654_321)
        context = MessageContext(
            topic="LiveMeasurement",
            partition=0,
            offset=0,
            size=source.ByteSize(),
        )

        with patch.object(application._producer, "produce_row") as produce_row:
            build_output_stream(application, Settings.from_environment({}), tracker).test(
                source,
                key=source.mrid.encode("utf-8"),
                timestamp=1_111,
                headers=[],
                ctx=context,
            )

        produce_row.assert_called_once()
        arguments = produce_row.call_args.kwargs
        serialized_request = arguments["row"].value.SerializeToString()
        request = measurement_session_pb2.MeasurementSessionRequest()
        request.ParseFromString(serialized_request)
        self.assertEqual(arguments["key"], request.session_id.encode("utf-8"))
        self.assertEqual(request.requested_at.ToMilliseconds(), 1_726_000_101_987)

    def test_builds_the_raw_protobuf_output_stream_and_at_least_once_application(self) -> None:
        settings = Settings.from_environment({})
        application = self._application()

        stream = build_output_stream(application, settings)
        processor_application, processor_stream = build_application(settings)

        self.assertIsNotNone(stream)
        self.assertIsInstance(processor_application, Application)
        self.assertIsNotNone(processor_stream)

    def _application(self) -> Application:
        return Application(
            broker_address="kafka:9092",
            consumer_group="processor-frequency-measurement-session-test",
            auto_create_topics=False,
        )


def _frequency(
    value: float,
    *,
    seconds: int,
    nanos: int = 0,
) -> rtd_schema_pb2.MCCSMeasurementValue:
    source = rtd_schema_pb2.MCCSMeasurementValue(
        mrid=FREQUENCY_MRID,
        double_value=value,
    )
    source.timestamp_mccs.seconds = seconds
    source.timestamp_mccs.nanos = nanos
    source.quality.valid = True
    return source


if __name__ == "__main__":
    unittest.main()