"""Tests for the framework-owned Quixstreams pipeline boundary."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from quixstreams import Application
from quixstreams.models.messagecontext import MessageContext

from wama_processor import (
    DerivedMeasurement,
    InputMeasurement,
    ProcessorDefinition,
    RuntimeConfig,
    build_output_stream,
    build_transformation_stream,
    run_processor,
)
from wama_processor.generated.rtd_schema_pb2 import MCCSMeasurementValue


class RuntimeTests(unittest.TestCase):
    """Keep raw-Protobuf keys and timestamps at the runtime boundary."""

    def test_forwards_the_source_timestamp_through_the_transformation_stream(self) -> None:
        application = self._application()
        definition = self._definition()
        source = self._source()

        result = build_transformation_stream(
            application,
            definition,
            self._config(),
        ).test(
            source,
            key=source.mrid.encode("utf-8"),
            timestamp=1_726_000_123_456,
            headers=[("trace-id", b"runtime")],
        )

        self.assertEqual(len(result), 1)
        derived, key, timestamp, headers = result[0]
        self.assertEqual(derived.mrid, "urn:wama:poc:test:frequency-millihertz")
        self.assertEqual(key, source.mrid.encode("utf-8"))
        self.assertEqual(timestamp, 1_726_000_123_456)
        self.assertEqual(headers, [("trace-id", b"runtime")])

    def test_publishes_the_derived_mrid_as_kafka_key(self) -> None:
        application = self._application()
        source = self._source()
        context = MessageContext(
            topic="LiveMeasurement",
            partition=0,
            offset=0,
            size=source.ByteSize(),
        )

        with patch.object(application._producer, "produce_row") as produce_row:
            build_output_stream(application, self._definition(), self._config()).test(
                source,
                key=source.mrid.encode("utf-8"),
                timestamp=1_726_000_123_456,
                headers=[],
                ctx=context,
            )

        produce_row.assert_called_once()
        arguments = produce_row.call_args.kwargs
        self.assertEqual(
            arguments["key"],
            b"urn:wama:poc:test:frequency-millihertz",
        )
        self.assertEqual(arguments["timestamp"], 1_726_000_123_456)

    def test_runs_the_output_stream(self) -> None:
        with patch.object(Application, "run", autospec=True) as run:
            run_processor(self._definition())

        run.assert_called_once()
        application, stream = run.call_args.args
        self.assertIsInstance(application, Application)
        self.assertIsNotNone(stream)

    def _definition(self) -> ProcessorDefinition:
        def scale_frequency(measurement: InputMeasurement) -> DerivedMeasurement | None:
            if measurement.double_value is None:
                return None
            return measurement.derive(
                "frequency_millihertz",
                measurement.double_value * 1_000,
            )

        return ProcessorDefinition(
            service_name="processor-runtime-test",
            inputs={"frequency_hz": "urn:wama:poc:test:frequency"},
            outputs={
                "frequency_millihertz": "urn:wama:poc:test:frequency-millihertz"
            },
            transform=scale_frequency,
        )

    def _application(self) -> Application:
        return Application(
            broker_address="kafka:9092",
            consumer_group="processor-runtime-test",
            auto_create_topics=False,
        )

    def _config(self) -> RuntimeConfig:
        return RuntimeConfig(
            kafka_bootstrap_servers="kafka:9092",
            consumer_group="processor-runtime-test",
            input_topic="LiveMeasurement",
            output_topic="LiveMeasurement",
        )

    def _source(self) -> MCCSMeasurementValue:
        source = MCCSMeasurementValue(
            mrid="urn:wama:poc:test:frequency",
            double_value=50.01,
        )
        source.quality.valid = True
        return source
