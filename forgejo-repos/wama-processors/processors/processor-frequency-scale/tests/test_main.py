"""Tests for Quixstreams metadata handling at the processor boundary."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from quixstreams import Application
from quixstreams.models.messagecontext import MessageContext

from processor_frequency_scale.config import ProcessorConfig
from processor_frequency_scale.generated.rtd_schema_pb2 import MCCSMeasurementValue
from processor_frequency_scale.main import (
    build_output_stream,
    build_transformation_stream,
    run_application,
)
from processor_frequency_scale.pipeline import OUTPUT_KEY, OUTPUT_MRID, SOURCE_KEY, SOURCE_MRID


class TransformationStreamTests(unittest.TestCase):
    """Ensure the stream explicitly retains source Kafka metadata."""

    def test_preserves_source_kafka_timestamp(self) -> None:
        application = Application(
            broker_address="kafka:9092",
            consumer_group="processor-frequency-scale-test",
            auto_create_topics=False,
        )
        config = ProcessorConfig(
            kafka_bootstrap_servers="kafka:9092",
            consumer_group="processor-frequency-scale-test",
            input_topic="LiveMeasurement",
            output_topic="LiveMeasurement",
        )
        source = MCCSMeasurementValue(mrid=SOURCE_MRID, double_value=50.01)
        source_timestamp_ms = 1_726_000_123_456

        result = build_transformation_stream(application, config).test(
            source,
            key=SOURCE_KEY,
            timestamp=source_timestamp_ms,
            headers=[("trace-id", b"frequency-scale")],
        )

        self.assertEqual(len(result), 1)
        transformed, key, timestamp, headers = result[0]
        self.assertEqual(transformed.mrid, OUTPUT_MRID)
        self.assertEqual(key, SOURCE_KEY)
        self.assertEqual(timestamp, source_timestamp_ms)
        self.assertEqual(headers, [("trace-id", b"frequency-scale")])

    def test_publishes_derived_key_with_source_timestamp(self) -> None:
        application = self._application()
        config = self._config()
        source = MCCSMeasurementValue(mrid=SOURCE_MRID, double_value=50.01)
        source_timestamp_ms = 1_726_000_123_456
        context = MessageContext(
            topic="LiveMeasurement",
            partition=0,
            offset=0,
            size=source.ByteSize(),
        )

        with patch.object(application._producer, "produce_row") as produce_row:
            build_output_stream(application, config).test(
                source,
                key=SOURCE_KEY,
                timestamp=source_timestamp_ms,
                headers=[],
                ctx=context,
            )

        produce_row.assert_called_once()
        arguments = produce_row.call_args.kwargs
        self.assertEqual(arguments["key"], OUTPUT_KEY)
        self.assertEqual(arguments["timestamp"], source_timestamp_ms)
        self.assertEqual(arguments["row"].value.mrid, OUTPUT_MRID)

    def test_runs_the_output_stream(self) -> None:
        with patch.object(Application, "run", autospec=True) as run:
            run_application(self._config())

        run.assert_called_once()
        application, stream = run.call_args.args
        self.assertIsInstance(application, Application)
        self.assertIsNotNone(stream)

    def _application(self) -> Application:
        return Application(
            broker_address="kafka:9092",
            consumer_group="processor-frequency-scale-test",
            auto_create_topics=False,
        )

    def _config(self) -> ProcessorConfig:
        return ProcessorConfig(
            kafka_bootstrap_servers="kafka:9092",
            consumer_group="processor-frequency-scale-test",
            input_topic="LiveMeasurement",
            output_topic="LiveMeasurement",
        )