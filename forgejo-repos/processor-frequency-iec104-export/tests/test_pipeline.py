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
    """Require event-time bucket timestamps and deterministic output key preservation."""

    def test_transformation_emits_closed_bucket_at_bucket_start(self) -> None:
        application = self._application()
        bucket_second = 1_726_000_123
        source = _frequency(2, bucket_second, 0)
        closing_source = _frequency(2, bucket_second + 1, 0)

        stream = build_transformation_stream(application, _settings())
        first = stream.test(
            source,
            key=source.mrid.encode("utf-8"),
            timestamp=1_726_000_999_999,
            headers=[("trace-id", b"frequency-iec104")],
        )
        result = stream.test(
            closing_source,
            key=closing_source.mrid.encode("utf-8"),
            timestamp=1_726_001_999_999,
            headers=[("trace-id", b"frequency-iec104")],
        )

        self.assertEqual(first, [])
        self.assertEqual(len(result), 1)
        record, key, timestamp, headers = result[0]
        self.assertEqual(key, closing_source.mrid.encode("utf-8"))
        self.assertEqual(timestamp, bucket_second * 1_000)
        self.assertEqual(headers, [("trace-id", b"frequency-iec104")])
        self.assertEqual(record.created_at.ToMilliseconds(), bucket_second * 1_000)
        self.assertEqual(record.iec104_asdu.common_address, 1002)

    def test_output_uses_export_id_key_and_bucket_start_timestamp(self) -> None:
        application = self._application()
        bucket_second = 1_726_000_123
        source = _frequency(1, bucket_second, 0)
        closing_source = _frequency(1, bucket_second + 1, 0)
        context = MessageContext(topic="LiveMeasurement", partition=0, offset=0, size=source.ByteSize())

        with patch.object(application._producer, "produce_row") as produce_row:
            stream = build_output_stream(application, _settings())
            stream.test(
                source,
                key=source.mrid.encode("utf-8"),
                timestamp=1_726_000_999_999,
                headers=[],
                ctx=context,
            )
            produce_row.assert_not_called()
            stream.test(
                closing_source,
                key=closing_source.mrid.encode("utf-8"),
                timestamp=1_726_001_999_999,
                headers=[],
                ctx=MessageContext(
                    topic="LiveMeasurement",
                    partition=0,
                    offset=1,
                    size=closing_source.ByteSize(),
                ),
            )

        produce_row.assert_called_once()
        arguments = produce_row.call_args.kwargs
        self.assertEqual(arguments["timestamp"], bucket_second * 1_000)
        self.assertEqual(arguments["key"], arguments["row"].value.export_id.encode("utf-8"))
        self.assertEqual(
            arguments["row"].value.created_at.ToMilliseconds(),
            bucket_second * 1_000,
        )

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


def _frequency(
    bay: int = 1,
    seconds: int = 1_726_000_123,
    nanos: int = 0,
) -> rtd_schema_pb2.MCCSMeasurementValue:
    source = rtd_schema_pb2.MCCSMeasurementValue(
        mrid=f"urn:wama:poc:pmu:bay-{bay:02}:frequency",
        double_value=50.01,
    )
    source.quality.valid = True
    source.timestamp_field.seconds = seconds
    source.timestamp_field.nanos = nanos
    return source


def _settings() -> Settings:
    return Settings.from_environment({})


if __name__ == "__main__":
    unittest.main()