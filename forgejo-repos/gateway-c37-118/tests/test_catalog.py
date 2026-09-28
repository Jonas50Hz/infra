"""Tests for a catalog whose reviewed source set may change."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from gateway_c37_118.catalog import CatalogError, load_catalog


class CatalogTests(unittest.TestCase):
    """Keep changing catalog membership strictly parsed."""

    def test_loads_added_and_removed_sources_without_a_fixed_fixture_count(
        self,
    ) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            source_directory = root / "sources"
            source_directory.mkdir()
            (root / "catalog.yaml").write_text("catalog_id: reviewed\n", encoding="utf-8")
            self._write_source(source_directory, "pmu-demo-one", 4712, 1001)
            self.assertEqual(
                [source.source_id for source in load_catalog(root).sources],
                ["pmu-demo-one"],
            )
            self._write_source(source_directory, "pmu-demo-two", 4713, 1002)
            self.assertEqual(
                [source.source_id for source in load_catalog(root).sources],
                ["pmu-demo-one", "pmu-demo-two"],
            )
            (source_directory / "pmu-demo-one.yaml").unlink()
            catalog = load_catalog(root)

            self.assertEqual([source.source_id for source in catalog.sources], ["pmu-demo-two"])
            self.assertEqual(
                catalog.mrids,
                frozenset({"urn:wama:demo:pmu:pmu-demo-two:frequency"}),
            )
            self._write_source(source_directory, "pmu-demo-one", 4712, 1001)
            self.assertEqual(
                [source.source_id for source in load_catalog(root).sources],
                ["pmu-demo-one", "pmu-demo-two"],
            )

    def test_rejects_unknown_source_yaml_fields(self) -> None:
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "sources").mkdir()
            (root / "catalog.yaml").write_text("catalog_id: reviewed\n", encoding="utf-8")
            (root / "sources" / "pmu-one.yaml").write_text(
                "source_id: pmu-one\nlocation: {site_id: one, display_name: One}\n"
                "connection: {ip_address: 127.0.0.1, port: 4712, pmu_idcode: 1001, wire_version: 2}\n"
                "signals: [{signal_id: frequency, source_channel: FREQ, mrid: urn:test, value_kind: double, quantity: frequency, unit: Hz, selector: {frequency: true}}]\n"
                "unreviewed: true\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(CatalogError, "unsupported"):
                load_catalog(root)

    @staticmethod
    def _write_source(
        source_directory: Path,
        source_id: str,
        port: int,
        pmu_idcode: int,
    ) -> None:
        (source_directory / f"{source_id}.yaml").write_text(
            f"""\
source_id: {source_id}
location: {{site_id: demo, display_name: Demo}}
connection: {{ip_address: 127.0.0.1, port: {port}, pmu_idcode: {pmu_idcode}, wire_version: 2}}
signals:
  - {{signal_id: frequency, source_channel: FREQ, mrid: "urn:wama:demo:pmu:{source_id}:frequency", value_kind: double, quantity: frequency, unit: Hz, selector: {{frequency: true}}}}
""",
            encoding="utf-8",
        )


if __name__ == "__main__":
    unittest.main()
