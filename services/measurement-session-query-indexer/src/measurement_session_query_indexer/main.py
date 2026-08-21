"""Entrypoint for the root-owned MeasurementSession Iceberg query indexer."""

from __future__ import annotations

import logging

import boto3
from botocore.config import Config
import psycopg

from measurement_session_query_indexer.artifact import ArtifactVerificationError, verify_artifact
from measurement_session_query_indexer.config import ConfigurationError, Settings
from measurement_session_query_indexer.consumer import BlobmetaQueryIndexer
from measurement_session_query_indexer.ledger import LedgerError, PostgresRegistrationLedger
from measurement_session_query_indexer.trino import (
    SessionWriter,
    TrinoClient,
    TrinoConnectionError,
    TrinoStatementError,
)


def main() -> None:
    """Initialize the mutable ledger then index every durable Blobmeta artifact."""

    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s %(message)s")
    client: TrinoClient | None = None
    try:
        settings = Settings.from_environment()
        storage_client = boto3.client(
            "s3",
            aws_access_key_id=settings.s3_access_key_id,
            aws_secret_access_key=settings.s3_secret_access_key,
            endpoint_url=settings.s3_endpoint_url,
            region_name=settings.s3_region,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )
        ledger = PostgresRegistrationLedger(settings.postgres_dsn)
        ledger.initialize()
        client = TrinoClient(settings.trino_writer_url, settings.trino_writer_user)
        BlobmetaQueryIndexer(
            settings,
            lambda result: verify_artifact(storage_client, result),
            ledger,
            SessionWriter(client),
        ).run()
    except (
        ArtifactVerificationError,
        ConfigurationError,
        LedgerError,
        OSError,
        psycopg.Error,
        TrinoConnectionError,
        TrinoStatementError,
        ValueError,
    ) as error:
        logging.getLogger(__name__).error("Measurement-session query indexer failed: %s", error)
        raise SystemExit(2) from error
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    main()