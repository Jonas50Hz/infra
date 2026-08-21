"""Construct the production MeasurementSession CSV exporter."""

from __future__ import annotations

from measurement_session_exporter.app import create_app
from measurement_session_exporter.config import Settings


app = create_app(Settings.from_environment())