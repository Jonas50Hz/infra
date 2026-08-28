"""Run the frequency-triggered measurement-session processor."""

from __future__ import annotations

import logging
import os

from processor_frequency_measurement_session.pipeline import run_processor


def main() -> None:
    """Configure process logging and start the Quixstreams pipeline."""

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    run_processor()


if __name__ == "__main__":
    main()