"""Start the frequency-scale processor through the shared WAMA runtime."""

from __future__ import annotations

import logging
import os
from wama_processor import run_processor

from processor_frequency_scale.processor import PROCESSOR


def main() -> None:
    """Configure logging and start the shared processor runtime."""

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    run_processor(PROCESSOR)


if __name__ == "__main__":
    main()