"""Entrypoint for root-owned Alarm-to-Alerta reconciliation."""

from __future__ import annotations

import logging
import os

from alarm_alerta_ingress.config import ConfigurationError, Settings
from alarm_alerta_ingress.consumer import AlarmIngressWorker


def main() -> None:
    """Configure logging and run the long-lived compacted Alarm consumer."""

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        AlarmIngressWorker(Settings.from_environment()).run()
    except ConfigurationError as error:
        logging.getLogger(__name__).error("Alarm Alerta ingress failed: %s", error)
        raise SystemExit(2) from error


if __name__ == "__main__":
    main()