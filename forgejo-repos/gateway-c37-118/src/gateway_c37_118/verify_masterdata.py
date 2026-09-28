"""Bounded proof that compacted Masterdata matches the reviewed gateway catalog."""

from __future__ import annotations

import os

from gateway_c37_118.catalog import load_catalog
from gateway_c37_118.masterdata import verify_catalog_projection
from gateway_c37_118.masterdata_publisher import current_masterdata


def main() -> None:
    """Read the compacted projection and require every approved source exactly once."""

    catalog = load_catalog(os.environ.get("WAMA_CATALOG_DIR", "/app/catalog"))
    topic = os.environ.get("WAMA_MASTERDATA_TOPIC", "Masterdata")
    bootstrap = os.environ.get("WAMA_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    revision = os.environ.get("WAMA_CATALOG_REVISION", "local")
    sources = verify_catalog_projection(
        current_masterdata(bootstrap, topic),
        catalog,
        revision,
    )
    print(f"Verified {len(sources)} catalog Masterdata sources.")


if __name__ == "__main__":
    main()
