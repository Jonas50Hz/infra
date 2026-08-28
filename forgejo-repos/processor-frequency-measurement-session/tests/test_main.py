"""Startup tests for the direct frequency measurement-session processor."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from quixstreams import Application

from processor_frequency_measurement_session.pipeline import run_processor


class MainTests(unittest.TestCase):
    """Require the configured output stream to reach Application.run."""

    def test_runs_measurement_session_output_stream(self) -> None:
        with patch.object(Application, "run", autospec=True) as run:
            run_processor()

        run.assert_called_once()
        application, stream = run.call_args.args
        self.assertIsInstance(application, Application)
        self.assertIsNotNone(stream)


if __name__ == "__main__":
    unittest.main()