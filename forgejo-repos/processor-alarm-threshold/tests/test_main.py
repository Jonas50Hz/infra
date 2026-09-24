"""Process entrypoint tests for graceful worker shutdown."""

from __future__ import annotations

import signal
import unittest
from unittest.mock import patch

from processor_alarm_threshold import main as main_module


class MainSignalTests(unittest.TestCase):
    """The process entrypoint delegates termination to its one worker."""

    def test_registers_sigterm_and_sigint_on_the_running_worker(self) -> None:
        events: list[str] = []
        handlers: dict[int, object] = {}
        settings = object()
        worker = _RecordingWorker(events)

        def register_handler(signum: int, handler: object) -> None:
            events.append("register")
            handlers[signum] = handler

        with (
            patch.object(main_module.logging, "basicConfig"),
            patch.object(main_module.Settings, "from_environment", return_value=settings),
            patch.object(main_module, "AlarmThresholdWorker", return_value=worker) as factory,
            patch.object(signal, "signal", side_effect=register_handler),
        ):
            main_module.main()

        factory.assert_called_once_with(settings)
        self.assertEqual(events, ["register", "register", "run"])
        self.assertEqual(list(handlers), [signal.SIGTERM, signal.SIGINT])

        sigterm_handler = handlers[signal.SIGTERM]
        sigint_handler = handlers[signal.SIGINT]
        self.assertTrue(callable(sigterm_handler))
        self.assertTrue(callable(sigint_handler))
        sigterm_handler(signal.SIGTERM, None)
        sigint_handler(signal.SIGINT, None)

        self.assertEqual(events, ["register", "register", "run", "stop", "stop"])


class _RecordingWorker:
    def __init__(self, events: list[str]) -> None:
        self._events = events

    def run(self) -> None:
        self._events.append("run")

    def stop(self) -> None:
        self._events.append("stop")


if __name__ == "__main__":
    unittest.main()