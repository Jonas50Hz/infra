"""Tests for the safe default application processor transformation."""

from __future__ import annotations

import unittest

from processor_template.generated.rtd_schema_pb2 import MCCSMeasurementValue
from processor_template.pipeline import transform


class PipelineTests(unittest.TestCase):
    """Ensure the starter cannot publish before customization."""

    def test_default_transform_drops_the_measurement(self) -> None:
        measurement = MCCSMeasurementValue(
            mrid="urn:wama:poc:test:input",
            double_value=50.0,
        )

        self.assertIsNone(transform(measurement))