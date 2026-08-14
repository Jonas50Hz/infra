"""Entry point for the retrying WAMA infrastructure readiness probe."""

from __future__ import annotations

import logging
from time import monotonic, sleep

from infra_readiness.checks import ReadinessError, check_all
from infra_readiness.config import ConfigurationError, Settings

LOGGER = logging.getLogger(__name__)


def run(settings: Settings) -> int:
    """Retry all behavioral checks until every dependency is ready or time expires."""

    deadline = monotonic() + settings.readiness_timeout_seconds
    attempt = 1
    while True:
        try:
            check_all(settings)
        except ReadinessError as error:
            remaining_seconds = deadline - monotonic()
            if remaining_seconds <= 0:
                LOGGER.error("Infrastructure readiness failed after %s attempt(s): %s", attempt, error)
                return 1
            LOGGER.warning(
                "Infrastructure readiness attempt %s failed: %s; retrying for %.0f seconds",
                attempt,
                error,
                remaining_seconds,
            )
            sleep(min(settings.readiness_retry_interval_seconds, remaining_seconds))
            attempt += 1
            continue

        LOGGER.info("Infrastructure readiness succeeded on attempt %s", attempt)
        return 0


def main() -> int:
    """Configure logging, load environment settings, and run the readiness gate."""

    logging.basicConfig(
        level="INFO",
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("kafka").setLevel(logging.WARNING)
    try:
        settings = Settings.from_environment()
    except ConfigurationError as error:
        LOGGER.error("Invalid infrastructure readiness configuration: %s", error)
        return 2
    return run(settings)


if __name__ == "__main__":
    raise SystemExit(main())