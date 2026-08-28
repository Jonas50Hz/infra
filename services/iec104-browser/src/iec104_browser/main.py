"""Construct the production persistent IEC 104 browser application."""

from __future__ import annotations

from pathlib import Path

from iec104_browser.app import create_app
from iec104_browser.config import Settings
from iec104_browser.hub import LiveHub
from iec104_browser.monitor import Iec104Monitor


def build_app():
    """Wire the browser hub to its process-lifetime c104 monitor and static UI."""

    settings = Settings.from_environment()

    def monitor_factory(event_callback, status_callback):
        return Iec104Monitor(
            settings.exporter_host,
            settings.exporter_port,
            event_callback,
            status_callback,
        )

    static_root = Path(__file__).resolve().parents[1] / "public"
    return create_app(LiveHub(monitor_factory, settings.queue_size), static_root)


app = build_app()