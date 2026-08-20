"""Tests for the shared runtime's safe processor defaults."""

from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from wama_processor import RuntimeConfig, RuntimeConfigurationError


class RuntimeConfigTests(unittest.TestCase):
    """Keep transport configuration out of the processor's calculation file."""

    def test_uses_wama_defaults_for_normal_processors(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            config = RuntimeConfig.from_environment("processor-frequency-scale")

        self.assertEqual(config.kafka_bootstrap_servers, "kafka:9092")
        self.assertEqual(config.consumer_group, "processor-frequency-scale")
        self.assertEqual(config.input_topic, "LiveMeasurement")
        self.assertEqual(config.output_topic, "LiveMeasurement")

    def test_rejects_blank_advanced_runtime_override(self) -> None:
        with patch.dict(
            os.environ,
            {"WAMA_INPUT_TOPIC": "   "},
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeConfigurationError, "WAMA_INPUT_TOPIC"):
                RuntimeConfig.from_environment("processor-frequency-scale")