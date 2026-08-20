"""Process entry point for LFR per-second preferred-frequency provision."""

from __future__ import annotations

import logging
import os

from processor_lfr_frequency_provision.runtime import run_processor


def main() -> None:
    """Configure structured logging and start the deadline-driven LFR runtime."""

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    run_processor()


if __name__ == "__main__":
    main()