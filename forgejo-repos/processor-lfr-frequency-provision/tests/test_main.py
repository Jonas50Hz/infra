"""Process entry-point tests for the LFR processor."""

from __future__ import annotations

import unittest
from unittest.mock import patch

from processor_lfr_frequency_provision.main import main


class MainTests(unittest.TestCase):
    """Require the container entry point to start the deadline-driven runtime."""

    def test_starts_the_lfr_runtime(self) -> None:
        with patch("processor_lfr_frequency_provision.main.run_processor") as run_processor:
            main()

        run_processor.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()