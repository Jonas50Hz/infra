"""Tests for the template Quixstreams entry point."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from quixstreams import Application

from processor_template.config import ProcessorConfig
from processor_template.main import run_application


class MainTests(unittest.TestCase):
    """Ensure the template passes its output stream to Quixstreams."""

    def test_runs_the_output_stream(self) -> None:
        config = ProcessorConfig(
            kafka_bootstrap_servers="kafka:9092",
            consumer_group="processor-template-test",
            input_topic="LiveMeasurement",
            output_topic="LiveMeasurement",
        )

        with patch.object(Application, "run", autospec=True) as run:
            run_application(config)

        run.assert_called_once()
        application, stream = run.call_args.args
        self.assertIsInstance(application, Application)
        self.assertIsNotNone(stream)