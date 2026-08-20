"""Quixstreams input-to-ExportRecord metadata propagation tests."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from quixstreams import Application
from quixstreams.models.messagecontext import MessageContext

from processor_frequency_iec104_export.config import Settings
from processor_frequency_iec104_export.generated import rtd_schema_pb2
from processor_frequency_iec104_export.pipeline import (
    build_application,
    build_output_stream,
    build_transformation_stream,
)


class PipelineTests(unittest.TestCase):
    """Require input Kafka timestamp and deterministic output key preservation."""

    def test_transformation_preserves_source_timestamp_and_headers(self) -> None:
        application = self._application()
        source = _frequency()
        timestamp_ms = 1_726_000_123_456

        result = build_transformation_stream(application, _settings()).test(
            source,
            key=source.mrid.encode("utf-8"),
            timestamp=timestamp_ms,
            headers=[("trace-id", b"frequency-iec104")],
        )

        self.assertEqual(len(result), 1)
        record, key, timestamp, headers = result[0]
        self.assertEqual(key, source.mrid.encode("utf-8"))
        self.assertEqual(timestamp, timestamp_ms)
        self.assertEqual(headers, [("trace-id", b"frequency-iec104")])
        self.assertEqual(record.created_at.ToMilliseconds(), timestamp_ms)

    def test_output_uses_export_id_key_and_source_timestamp(self) -> None:
        application = self._application()
        source = _frequency()
        timestamp_ms = 1_726_000_123_456
        context = MessageContext(topic="LiveMeasurement", partition=0, offset=0, size=source.ByteSize())

        with patch.object(application._producer, "produce_row") as produce_row:
            build_output_stream(application, _settings()).test(
                source,
                key=source.mrid.encode("utf-8"),
                timestamp=timestamp_ms,
                headers=[],
                ctx=context,
            )

        produce_row.assert_called_once()
        arguments = produce_row.call_args.kwargs
        self.assertEqual(arguments["timestamp"], timestamp_ms)
        self.assertEqual(arguments["key"], arguments["row"].value.export_id.encode("utf-8"))
        self.assertEqual(arguments["row"].value.created_at.ToMilliseconds(), timestamp_ms)

    def test_builds_at_least_once_application(self) -> None:
        application, stream = build_application(_settings())

        self.assertIsInstance(application, Application)
        self.assertIsNotNone(stream)

    def _application(self) -> Application:
        return Application(
            broker_address="kafka:9092",
            consumer_group="processor-frequency-iec104-export-test",
            auto_create_topics=False,
        )


def _frequency() -> rtd_schema_pb2.MCCSMeasurementValue:
    source = rtd_schema_pb2.MCCSMeasurementValue(
        mrid="urn:wama:poc:pmu:bay-01:frequency",
        double_value=50.01,
    )
    source.quality.valid = True
    return source


def _settings() -> Settings:
    return Settings.from_environment({})


if __name__ == "__main__":
    unittest.main()