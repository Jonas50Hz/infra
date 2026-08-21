"""Atomic publication of generated gateway Grafana dashboard files."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
from collections.abc import Iterable, Mapping
from typing import Any

from gateway_dashboard_provisioner.model import GatewaySource
from gateway_dashboard_provisioner.render import render_snapshot


_MANAGED_FILENAME = re.compile(r"(?:fleet|source-[0-9a-f]{24})\.json\Z")


class DashboardStore:
    """Own only generated gateway files inside Grafana's dedicated directory."""

    def __init__(self, directory: str | Path) -> None:
        self._directory = Path(directory)

    def publish(self, sources: Iterable[GatewaySource]) -> tuple[str, ...]:
        """Render and replace the complete active-source dashboard snapshot."""

        return self.publish_snapshot(render_snapshot(sources))

    def publish_snapshot(self, snapshot: Mapping[str, Mapping[str, Any]]) -> tuple[str, ...]:
        """Atomically replace each rendered file and remove stale managed files."""

        filenames = tuple(sorted(snapshot))
        if any(not _MANAGED_FILENAME.fullmatch(filename) for filename in filenames):
            raise ValueError("Gateway dashboard snapshot contains an unsafe filename")

        self._directory.mkdir(parents=True, exist_ok=True)
        for filename in filenames:
            self._replace_file(filename, snapshot[filename])

        for path in self._directory.iterdir():
            if path.name not in filenames and path.is_file() and _MANAGED_FILENAME.fullmatch(path.name):
                path.unlink()
        return filenames

    def _replace_file(self, filename: str, dashboard: Mapping[str, Any]) -> None:
        destination = self._directory / filename
        temporary = self._directory / f".{filename}.tmp"
        payload = json.dumps(dashboard, indent=2, sort_keys=True) + "\n"
        with temporary.open("w", encoding="utf-8") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, destination)