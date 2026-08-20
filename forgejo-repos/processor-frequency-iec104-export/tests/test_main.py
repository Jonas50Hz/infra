"""Process startup tests for the direct frequency IEC 104 export processor."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from quixstreams import Application

from processor_frequency_iec104_export.pipeline import run_processor


class MainTests(unittest.TestCase):
    """Require the direct pipeline output stream to reach Application.run."""

    def test_runs_export_output_stream(self) -> None:
        with patch.object(Application, "run", autospec=True) as run:
            run_processor()

        run.assert_called_once()
        application, stream = run.call_args.args
        self.assertIsInstance(application, Application)
        self.assertIsNotNone(stream)


if __name__ == "__main__":
    unittest.main()