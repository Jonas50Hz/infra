"""Construct the production MeasurementSession submission service."""

from __future__ import annotations

from pathlib import Path

from measurement_session_api.app import create_app
from measurement_session_api.config import Settings
from measurement_session_api.publisher import KafkaSessionPublisher


def build_app():
    """Wire the trusted local request surface to the Kafka publisher."""

    settings = Settings.from_environment()
    static_root = Path(__file__).resolve().parents[1] / "public"
    return create_app(settings, KafkaSessionPublisher(settings), static_root=static_root)


app = build_app()