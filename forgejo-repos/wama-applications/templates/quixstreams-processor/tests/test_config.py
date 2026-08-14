"""Tests for the application processor YAML contract."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from processor_template.config import ConfigurationError, load_config


class ProcessorConfigTests(unittest.TestCase):
    """Keep application processor configuration errors outside the Kafka runtime path."""

    def test_loads_required_kafka_settings(self) -> None:
        config = self._load(
            """
kafka_bootstrap_servers: kafka:9092
consumer_group: processor-frequency
input_topic: LiveMeasurement
output_topic: LiveMeasurement
"""
        )

        self.assertEqual(config.kafka_bootstrap_servers, "kafka:9092")
        self.assertEqual(config.consumer_group, "processor-frequency")

    def test_rejects_unknown_configuration_keys(self) -> None:
        with self.assertRaisesRegex(
            ConfigurationError,
            "unsupported key\\(s\\): custom_runtime",
        ):
            self._load(
                """
kafka_bootstrap_servers: kafka:9092
consumer_group: processor-frequency
input_topic: LiveMeasurement
output_topic: LiveMeasurement
custom_runtime: unsupported
"""
            )

    def _load(self, contents: str):
        with TemporaryDirectory() as directory:
            config_path = Path(directory) / "processor.yaml"
            config_path.write_text(contents, encoding="utf-8")
            return load_config(config_path)