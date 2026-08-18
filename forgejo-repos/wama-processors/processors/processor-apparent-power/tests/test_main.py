"""Tests for apparent-power Kafka metadata and output handling."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from quixstreams import Application
from quixstreams.models.messagecontext import MessageContext

from processor_apparent_power.config import ProcessorConfig
from processor_apparent_power.generated.rtd_schema_pb2 import MCCSMeasurementValue
from processor_apparent_power.main import (
    build_output_stream,
    build_transformation_stream,
    run_application,
)
from processor_apparent_power.pipeline import OUTPUT_MRIDS, PhaseCache


class TransformationStreamTests(unittest.TestCase):
    """Ensure the triggering source Kafka timestamp reaches the output record."""

    def test_preserves_triggering_kafka_timestamp(self) -> None:
        cache = PhaseCache()
        application = self._application()
        stream = build_transformation_stream(application, self._config(), cache)
        voltage = self._measurement("voltage-l2", 229.8)
        current = self._measurement("current-l2", 316.7)

        self.assertEqual(
            stream.test(voltage, voltage.mrid.encode(), 1_000, headers=[]),
            [],
        )
        result = stream.test(current, current.mrid.encode(), 2_000, headers=[])

        self.assertEqual(len(result), 1)
        derived, key, timestamp, headers = result[0]
        self.assertEqual(derived.mrid, OUTPUT_MRIDS["l2"])
        self.assertEqual(key, current.mrid.encode())
        self.assertEqual(timestamp, 2_000)
        self.assertEqual(headers, [])

    def test_publishes_derived_key_with_triggering_timestamp(self) -> None:
        cache = PhaseCache()
        application = self._application()
        stream = build_output_stream(application, self._config(), cache)
        voltage = self._measurement("voltage-l3", 230.1)
        current = self._measurement("current-l3", 317.4)
        context = MessageContext(
            topic="LiveMeasurement",
            partition=0,
            offset=0,
            size=voltage.ByteSize(),
        )

        with patch.object(application._producer, "produce_row") as produce_row:
            stream.test(
                voltage,
                voltage.mrid.encode(),
                1_000,
                headers=[],
                ctx=context,
            )
            stream.test(
                current,
                current.mrid.encode(),
                2_000,
                headers=[],
                ctx=context,
            )

        produce_row.assert_called_once()
        arguments = produce_row.call_args.kwargs
        self.assertEqual(arguments["key"], OUTPUT_MRIDS["l3"].encode())
        self.assertEqual(arguments["timestamp"], 2_000)
        self.assertEqual(arguments["row"].value.mrid, OUTPUT_MRIDS["l3"])

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
            consumer_group="processor-apparent-power-test",
            auto_create_topics=False,
        )

    def _config(self) -> ProcessorConfig:
        return ProcessorConfig(
            kafka_bootstrap_servers="kafka:9092",
            consumer_group="processor-apparent-power-test",
            input_topic="LiveMeasurement",
            output_topic="LiveMeasurement",
        )

    def _measurement(self, source_name: str, value: float) -> MCCSMeasurementValue:
        measurement = MCCSMeasurementValue(
            mrid=f"urn:wama:poc:pmu:bay-01:{source_name}",
            double_value=value,
        )
        measurement.quality.valid = True
        return measurement