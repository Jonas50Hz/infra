"""Run the one-way IEC 104 exporter process."""

from __future__ import annotations

import logging
from pathlib import Path
import signal
from threading import Event

from iec104_exporter.config import Settings
from iec104_exporter.consumer import ExportWorker
from iec104_exporter.transport import C104Transport


def main() -> None:
    """Start c104, consume Export records, and stop cleanly on termination."""

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.from_environment()
    ready_file = Path(settings.ready_file)
    shutdown = Event()

    def request_shutdown(_signal_number: int, _frame: object) -> None:
        shutdown.set()

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)

    transport = C104Transport(settings.bind_host, settings.port, settings.backend_port)
    worker = ExportWorker(settings, transport)
    try:
        transport.start()
        ready_file.parent.mkdir(parents=True, exist_ok=True)
        ready_file.touch()
        worker.start()
        while not shutdown.wait(1):
            if worker.failure is not None:
                raise RuntimeError(f"IEC 104 exporter worker failed: {worker.failure}")
    finally:
        ready_file.unlink(missing_ok=True)
        worker.stop()
        transport.stop()


if __name__ == "__main__":
    main()