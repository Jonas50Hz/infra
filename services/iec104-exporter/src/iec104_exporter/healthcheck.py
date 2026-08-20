"""Container health check for the started IEC 104 listener."""

from __future__ import annotations

import os
from pathlib import Path


def main() -> None:
    """Return success only after the exporter process has started c104."""

    ready_file = Path(os.environ.get("IEC104_READY_FILE", "/tmp/iec104-exporter-ready"))
    if not ready_file.is_file():
        raise SystemExit("IEC 104 exporter listener is not ready")


if __name__ == "__main__":
    main()