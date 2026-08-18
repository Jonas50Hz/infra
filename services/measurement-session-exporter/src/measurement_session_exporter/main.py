"""Run one explicit static finalized-session export."""

from __future__ import annotations

import logging

import boto3
from botocore.config import Config
from kafka import KafkaProducer

from measurement_session_exporter.config import ConfigurationError, ExporterSettings, load_fixture
from measurement_session_exporter.export import export_finalized_session

LOGGER = logging.getLogger(__name__)


def main() -> None:
    """Load the final fixture, upload immutable evidence, and publish it once."""

    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        settings = ExporterSettings.from_environment()
        fixture = load_fixture(settings.fixture_path, settings.session_id_override)
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=settings.s3_access_key_id,
            aws_secret_access_key=settings.s3_secret_access_key,
            endpoint_url=settings.s3_endpoint_url,
            region_name=settings.s3_region,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )
        producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers.split(","),
            acks="all",
            retries=5,
            request_timeout_ms=30_000,
        )
        try:
            result = export_finalized_session(
                fixture,
                s3_client,
                producer,
                settings.kafka_topic,
                settings.s3_bucket,
            )
        finally:
            producer.close(timeout=30)
    except (ConfigurationError, OSError, ValueError) as error:
        LOGGER.error("Finalized-session export failed: %s", error)
        raise SystemExit(2) from error

    LOGGER.info("Published finalized MeasurementSession %s (%d bytes)", result.session.session_id, len(result.payload))


if __name__ == "__main__":
    main()