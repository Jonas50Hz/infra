"""Entrypoint for the root-owned Blobmeta PostgreSQL materializer."""

from __future__ import annotations

import logging

import psycopg

from blobmeta_catalog.catalog import CatalogError, PostgresCatalog
from blobmeta_catalog.config import ConfigurationError, Settings
from blobmeta_catalog.consumer import BlobmetaWorker


def main() -> None:
    """Initialize app-owned PostgreSQL metadata tables and consume Blobmeta."""

    logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s %(name)s %(message)s")
    try:
        settings = Settings.from_environment()
        catalog = PostgresCatalog(settings.postgres_dsn)
        catalog.initialize()
        BlobmetaWorker(settings, catalog).run()
    except (ConfigurationError, CatalogError, psycopg.Error, OSError, ValueError) as error:
        logging.getLogger(__name__).error("Blobmeta catalog failed: %s", error)
        raise SystemExit(2) from error


if __name__ == "__main__":
    main()