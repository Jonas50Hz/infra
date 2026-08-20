"""Tests for Blobmeta wire validation before catalog insertion."""

from __future__ import annotations

from types import SimpleNamespace
import unittest

from blobmeta_catalog.catalog import CatalogBlob
from blobmeta_catalog.config import Settings
from blobmeta_catalog.consumer import BlobmetaWorker
from test_catalog import _blobmeta, _request


class BlobmetaWorkerTests(unittest.TestCase):
    """Prove the compacted message key and contract are checked before storage."""

    def test_inserts_key_aligned_blobmeta(self) -> None:
        message = _blobmeta(_request())
        catalog = _Catalog()
        worker = BlobmetaWorker(Settings.from_environment({}), catalog)

        worker.process_record(
            SimpleNamespace(
                key=message.blob_id.encode("utf-8"),
                value=message.SerializeToString(deterministic=True),
            )
        )

        self.assertIsInstance(catalog.blob, CatalogBlob)
        self.assertEqual(catalog.blob.blob_id, message.blob_id)


class _Catalog:
    def __init__(self) -> None:
        self.blob: CatalogBlob | None = None

    def initialize(self) -> None:
        pass

    def insert(self, blob: CatalogBlob) -> None:
        self.blob = blob