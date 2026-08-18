"""Command-line entry point for Druid supervisor initialization."""

from __future__ import annotations

import logging
from time import monotonic, sleep

import requests

from druid_init.config import ConfigurationError, Settings
from druid_init.supervisor import SupervisorError, check_supervisor, load_supervisor_spec, submit_supervisor

LOGGER = logging.getLogger(__name__)


def run(settings: Settings) -> int:
    """Retry an idempotent supervisor submission until Druid reports it healthy."""

    specification = load_supervisor_spec(settings.supervisor_spec_path, settings.supervisor_id)
    deadline = monotonic() + settings.timeout_seconds
    attempt = 1
    session = requests.Session()
    try:
        while True:
            try:
                submit_supervisor(session, settings.router_url, specification)
                check_supervisor(session, settings.router_url, settings.supervisor_id)
            except SupervisorError as error:
                remaining_seconds = deadline - monotonic()
                if remaining_seconds <= 0:
                    LOGGER.error("Druid supervisor initialization failed after %s attempt(s): %s", attempt, error)
                    return 1
                LOGGER.warning(
                    "Druid supervisor initialization attempt %s failed: %s; retrying for %.0f seconds",
                    attempt,
                    error,
                    remaining_seconds,
                )
                sleep(min(settings.retry_interval_seconds, remaining_seconds))
                attempt += 1
                continue
            LOGGER.info("Druid supervisor initialized on attempt %s", attempt)
            return 0
    finally:
        session.close()


def main() -> int:
    """Configure logging, load settings, and initialize the supervisor."""

    logging.basicConfig(
        level="INFO",
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        settings = Settings.from_environment()
        return run(settings)
    except (ConfigurationError, SupervisorError) as error:
        LOGGER.error("Invalid Druid supervisor initialization: %s", error)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())