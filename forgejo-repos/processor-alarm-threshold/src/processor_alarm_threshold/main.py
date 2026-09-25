"""Run the direct stateful alarm threshold processor."""

from __future__ import annotations

import logging
import os
import signal
from types import FrameType

from processor_alarm_threshold.config import Settings
from processor_alarm_threshold.worker import AlarmThresholdWorker


def main() -> None:
    """Configure process logging and run the serialized Kafka worker."""

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    worker = AlarmThresholdWorker(Settings.from_environment())

    def request_stop(_signal_number: int, _frame: FrameType | None) -> None:
        worker.stop()

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    worker.run()


if __name__ == "__main__":
    main()