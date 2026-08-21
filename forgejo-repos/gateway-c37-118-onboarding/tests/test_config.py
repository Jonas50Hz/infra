"""Tests for strict Git catalog validation."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import yaml

from gateway_c37_118_onboarding.config import CatalogError, load_catalog


class CatalogTests(unittest.TestCase):
    """Keep source identity and signal semantics deterministic before publication."""

    def test_loads_sorted_valid_sources(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            self._write(root, "pmu-bay-02", self._source("pmu-bay-02", "192.0.2.11", 1002))
            self._write(root, "pmu-bay-01", self._source("pmu-bay-01", "192.0.2.10", 1001))

            catalog = load_catalog(root, "wama-c37-118-onboarding", "abc123")

        self.assertEqual([source.source_id for source in catalog.sources], ["pmu-bay-01", "pmu-bay-02"])
        self.assertEqual(catalog.sources[0].signals[0].signal_id, "frequency")
        self.assertEqual(catalog.sources[0].wire_version, 2)
        self.assertEqual(catalog.sources[0].signals[0].c37_118_v2_selector.kind, "frequency")

    def test_allows_empty_catalog_for_complete_decommissioning(self) -> None:
        with TemporaryDirectory() as directory:
            catalog = load_catalog(directory, "wama-c37-118-onboarding", "abc123")

        self.assertEqual(catalog.sources, ())

    def test_rejects_duplicate_mrids_across_sources(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            first = self._source("pmu-bay-01", "192.0.2.10", 1001)
            second = self._source("pmu-bay-02", "192.0.2.11", 1002)
            second["signals"][0]["mrid"] = first["signals"][0]["mrid"]
            self._write(root, "pmu-bay-01", first)
            self._write(root, "pmu-bay-02", second)

            with self.assertRaisesRegex(CatalogError, "Duplicate MRID"):
                load_catalog(root, "wama-c37-118-onboarding", "abc123")

    def test_rejects_non_literal_endpoint_and_unknown_keys(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = self._source("pmu-bay-01", "pmu.example.test", 1001)
            source["unexpected"] = True
            self._write(root, "pmu-bay-01", source)

            with self.assertRaisesRegex(CatalogError, "unsupported key"):
                load_catalog(root, "wama-c37-118-onboarding", "abc123")

            del source["unexpected"]
            self._write(root, "pmu-bay-01", source)
            with self.assertRaisesRegex(CatalogError, "literal IPv4 or IPv6"):
                load_catalog(root, "wama-c37-118-onboarding", "abc123")

    def test_rejects_invalid_quantity_unit_mapping(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = self._source("pmu-bay-01", "192.0.2.10", 1001)
            source["signals"][0]["unit"] = "V"
            self._write(root, "pmu-bay-01", source)

            with self.assertRaisesRegex(CatalogError, "requires unit 'Hz'"):
                load_catalog(root, "wama-c37-118-onboarding", "abc123")

    def test_rejects_non_v2_connections_and_invalid_selectors(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            source = self._source("pmu-bay-01", "192.0.2.10", 1001)
            source["connection"]["wire_version"] = 3
            self._write(root, "pmu-bay-01", source)

            with self.assertRaisesRegex(CatalogError, "wire_version must be 2"):
                load_catalog(root, "wama-c37-118-onboarding", "abc123")

            source["connection"]["wire_version"] = 2
            source["signals"][0]["c37_118_v2_selector"] = {"frequency": False}
            self._write(root, "pmu-bay-01", source)
            with self.assertRaisesRegex(CatalogError, "must be true"):
                load_catalog(root, "wama-c37-118-onboarding", "abc123")

    @staticmethod
    def _write(root: Path, source_id: str, source: dict[str, object]) -> None:
        (root / f"{source_id}.yaml").write_text(
            yaml.safe_dump(source, sort_keys=False),
            encoding="utf-8",
        )

    @staticmethod
    def _source(source_id: str, address: str, pmu_idcode: int) -> dict[str, object]:
        return {
            "source_id": source_id,
            "location": {
                "site_id": "wama-poc-bay-01",
                "display_name": "WAMA PoC Bay 01",
            },
            "connection": {
                "protocol": "c37_118_tcp",
                "ip_address": address,
                "port": 4712,
                "pmu_idcode": pmu_idcode,
                "wire_version": 2,
            },
            "signals": [
                {
                    "signal_id": "frequency",
                    "source_channel": "FREQ",
                    "mrid": f"urn:wama:poc:pmu:{source_id}:frequency",
                    "value_kind": "double",
                    "quantity": "frequency",
                    "unit": "Hz",
                    "c37_118_v2_selector": {"frequency": True},
                }
            ],
        }