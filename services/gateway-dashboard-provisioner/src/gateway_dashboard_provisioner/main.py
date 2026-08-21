"""Entrypoint for root-owned Masterdata-driven gateway dashboard provisioning."""

from __future__ import annotations

import logging

from gateway_dashboard_provisioner.config import ConfigurationError, Settings
from gateway_dashboard_provisioner.consumer import MasterdataDashboardWorker
from gateway_dashboard_provisioner.storage import DashboardStore


def main() -> None:
    """Run dashboard reconciliation until Compose stops the root service."""

    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        settings = Settings.from_environment()
        MasterdataDashboardWorker(settings, DashboardStore(settings.dashboard_directory)).run()
    except ConfigurationError as error:
        logging.getLogger(__name__).error("Gateway dashboard provisioner failed: %s", error)
        raise SystemExit(2) from error


if __name__ == "__main__":
    main()