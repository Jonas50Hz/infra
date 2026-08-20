"""Entrypoint for the root-owned MeasurementSession processor."""

from __future__ import annotations

import logging

import boto3
from botocore.config import Config
from kafka import KafkaProducer

from measurement_session_processor.config import ConfigurationError, Settings
from measurement_session_processor.druid import DruidClient
from measurement_session_processor.storage import SeaweedSessionStore
from measurement_session_processor.worker import SessionProcessingError, SessionWorker


def main() -> None:
    """Create network clients and run the persistent manual-commit worker."""

    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        settings = Settings.from_environment()
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
        worker = SessionWorker(
            settings,
            DruidClient(
                settings.druid_router_url,
                settings.druid_datasource,
                settings.druid_query_timeout_seconds,
                settings.max_rows,
            ),
            SeaweedSessionStore(s3_client),
            producer,
        )
        try:
            worker.run()
        finally:
            producer.close(timeout=30)
    except (ConfigurationError, SessionProcessingError, OSError, ValueError) as error:
        logging.getLogger(__name__).error("MeasurementSession processor failed: %s", error)
        raise SystemExit(2) from error


if __name__ == "__main__":
    main()