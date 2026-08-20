"""Tests for the framework-owned authoring API contract."""

from __future__ import annotations

import unittest

from wama_processor import (
    DerivedMeasurement,
    InputMeasurement,
    ProcessorDefinition,
    ProcessorDefinitionError,
)
from wama_processor.generated.rtd_schema_pb2 import MCCSMeasurementValue


class ProcessorDefinitionTests(unittest.TestCase):
    """Keep source admission and output safety outside EE-authored processors."""

    def test_admits_a_declared_input_and_preserves_its_context(self) -> None:
        definition = self._definition()
        source = self._measurement("urn:wama:poc:test:frequency", 50.01)
        source_bytes = source.SerializeToString()

        derived = definition.transform_record(
            source,
            source.mrid.encode("utf-8"),
            1_726_000_123_456,
        )

        self.assertIsNotNone(derived)
        assert derived is not None
        self.assertEqual(derived.name, "frequency_millihertz")
        self.assertEqual(derived.double_value, 50_010.0)
        self.assertEqual(
            derived.protobuf.mrid,
            "urn:wama:poc:test:frequency-millihertz",
        )
        self.assertEqual(derived.protobuf.timestamp_mccs, source.timestamp_mccs)
        self.assertEqual(derived.protobuf.quality, source.quality)
        self.assertEqual(derived.kafka_timestamp_ms, 1_726_000_123_456)
        self.assertEqual(source.SerializeToString(), source_bytes)

    def test_rejects_wrong_keys_unrelated_records_and_own_outputs(self) -> None:
        definition = self._definition()
        source = self._measurement("urn:wama:poc:test:frequency", 50.01)
        unrelated = self._measurement("urn:wama:poc:test:voltage", 230.4)
        own_output = self._measurement(
            "urn:wama:poc:test:frequency-millihertz",
            50_010.0,
        )

        self.assertIsNone(definition.transform_record(source, b"wrong-key", 1))
        self.assertIsNone(
            definition.transform_record(unrelated, unrelated.mrid.encode("utf-8"), 2)
        )
        self.assertIsNone(
            definition.transform_record(own_output, own_output.mrid.encode("utf-8"), 3)
        )

    def test_rejects_shared_input_and_output_mrids(self) -> None:
        with self.assertRaisesRegex(ProcessorDefinitionError, "distinct MRIDs"):
            ProcessorDefinition(
                service_name="processor-invalid",
                inputs={"source": "urn:wama:poc:test:source"},
                outputs={"output": "urn:wama:poc:test:source"},
                transform=lambda measurement: None,
            )

    def test_rejects_a_handcrafted_output_with_the_wrong_mrid(self) -> None:
        def invalid_transform(measurement: InputMeasurement) -> DerivedMeasurement:
            copied = MCCSMeasurementValue()
            copied.CopyFrom(measurement._source)
            copied.mrid = "urn:wama:poc:test:not-declared"
            return DerivedMeasurement(
                name="frequency_millihertz",
                double_value=50_010.0,
                _measurement=copied,
                kafka_timestamp_ms=measurement.kafka_timestamp_ms,
            )

        definition = ProcessorDefinition(
            service_name="processor-invalid",
            inputs={"frequency_hz": "urn:wama:poc:test:frequency"},
            outputs={"frequency_millihertz": "urn:wama:poc:test:frequency-millihertz"},
            transform=invalid_transform,
        )
        source = self._measurement("urn:wama:poc:test:frequency", 50.01)

        with self.assertRaisesRegex(ProcessorDefinitionError, "declared output"):
            definition.transform_record(source, source.mrid.encode("utf-8"), 1)

    def _definition(self) -> ProcessorDefinition:
        def scale_frequency(measurement: InputMeasurement) -> DerivedMeasurement | None:
            if measurement.double_value is None:
                return None
            return measurement.derive(
                "frequency_millihertz",
                measurement.double_value * 1_000,
            )

        return ProcessorDefinition(
            service_name="processor-frequency-scale",
            inputs={"frequency_hz": "urn:wama:poc:test:frequency"},
            outputs={
                "frequency_millihertz": "urn:wama:poc:test:frequency-millihertz"
            },
            transform=scale_frequency,
        )

    def _measurement(self, mrid: str, double_value: float) -> MCCSMeasurementValue:
        measurement = MCCSMeasurementValue(mrid=mrid, double_value=double_value)
        measurement.timestamp_mccs.seconds = 1_726_000_123
        measurement.quality.valid = True
        return measurement
