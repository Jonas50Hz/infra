"""Construct the production catalog API process from environment settings."""

from __future__ import annotations

import boto3
from botocore.config import Config

from measurement_session_api.app import create_app
from measurement_session_api.catalog import PostgresCatalog
from measurement_session_api.config import Settings
from measurement_session_api.consumer import CatalogWorker
from measurement_session_api.storage import SeaweedSessionStore


def build_app():
    """Wire private Kafka/PostgreSQL/S3 clients behind anonymous HTTP routes."""

    settings = Settings.from_environment()
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=settings.s3_access_key_id,
        aws_secret_access_key=settings.s3_secret_access_key,
        endpoint_url=settings.s3_endpoint_url,
        region_name=settings.s3_region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )
    catalog = PostgresCatalog(settings.postgres_dsn)
    return create_app(catalog, SeaweedSessionStore(s3_client), CatalogWorker(settings, catalog))


app = build_app()