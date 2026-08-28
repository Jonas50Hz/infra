"""Tests for deterministic gateway Grafana dashboard rendering."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from urllib.parse import parse_qs, urlparse

from gateway_dashboard_provisioner.model import GatewaySignal, GatewaySource
from gateway_dashboard_provisioner.render import (
    FLEET_DASHBOARD_UID,
    dashboard_uid,
    render_fleet_dashboard,
    render_gateway_dashboard,
    render_snapshot,
    source_dashboard_filename,
)
from gateway_dashboard_provisioner.storage import DashboardStore
from gateway_dashboard_provisioner.state import GatewayRegistry


class DashboardRenderTests(unittest.TestCase):
    """Keep generated Grafana JSON stable, unit-safe, and source-scoped."""

    def test_renders_unit_safe_source_panels_and_escaped_mrids(self) -> None:
        dashboard = render_gateway_dashboard(_source())

        self.assertEqual(dashboard["uid"], dashboard_uid("pmu-bay-01"))
        self.assertEqual(dashboard["title"], "WAMA Gateway: WAMA PoC Bay 01")
        panels = {panel["title"]: panel for panel in dashboard["panels"]}
        self.assertEqual(panels["Phase Voltages"]["fieldConfig"]["defaults"]["unit"], "volt")
        self.assertEqual(panels["Phase Currents"]["fieldConfig"]["defaults"]["unit"], "amp")
        self.assertEqual(panels["Frequency"]["fieldConfig"]["defaults"]["unit"], "hertz")
        self.assertEqual(panels["ROCOF (Hz/s)"]["fieldConfig"]["defaults"]["unit"], "suffix:Hz/s")
        query = panels["Phase Voltages"]["targets"][0]["builder"]["query"]
        self.assertIn("urn:wama:poc:pmu:bay-01:voltage-o''hara", query)
        self.assertEqual(
            panels["Phase Voltages"]["datasource"]["uid"],
            "druid",
        )

    def test_renders_conservative_quality_records_with_explicit_latest_quality(self) -> None:
        dashboard = render_gateway_dashboard(_source())
        panels = {panel["title"]: panel for panel in dashboard["panels"]}

        series_query = panels["Phase Voltages"]["targets"][0]["builder"]["query"]
        freshness_query = panels["Last Measurement"]["targets"][0]["builder"]["query"]
        latest_query = panels["Latest Records"]["targets"][0]["builder"]["query"]

        self.assertNotIn('"quality_valid" = \'true\'', series_query)
        self.assertNotIn('"quality_valid" = \'true\'', freshness_query)
        self.assertNotIn('"quality_valid" = \'true\'', latest_query)
        self.assertIn('"quality_valid"', latest_query)

    def test_scopes_ordered_series_context_without_changing_rendered_sql(self) -> None:
        dashboard = render_gateway_dashboard(_source())
        panels = {panel["title"]: panel for panel in dashboard["panels"]}

        series_target = panels["Phase Voltages"]["targets"][0]
        freshness_target = panels["Last Measurement"]["targets"][0]
        latest_records_target = panels["Latest Records"]["targets"][0]

        self.assertEqual(
            series_target["settings"]["contextParameters"],
            [{"name": "maxSegmentPartitionsOrderedInMemory", "value": 75}],
        )
        self.assertEqual(freshness_target["settings"]["contextParameters"], [])
        self.assertEqual(latest_records_target["settings"]["contextParameters"], [])
        self.assertEqual(
            series_target["builder"]["query"],
            'SELECT "__time", CASE "mrid" '
            "WHEN 'urn:wama:poc:pmu:bay-01:voltage-o''hara' THEN 'voltage-l1' "
            'END AS "signal", "double_value" '
            'FROM "live_measurements" '
            "WHERE \"mrid\" IN ('urn:wama:poc:pmu:bay-01:voltage-o''hara') "
            'AND "double_value" IS NOT NULL '
            'AND "__time" >= MILLIS_TO_TIMESTAMP(${__from}) '
            'AND "__time" <= MILLIS_TO_TIMESTAMP(${__to}) '
            'ORDER BY "__time" ASC',
        )
        self.assertEqual(
            freshness_target["builder"]["query"],
            'SELECT MAX("__time") AS "last_measurement" '
            'FROM "live_measurements" '
            "WHERE \"mrid\" IN ('urn:wama:poc:pmu:bay-01:voltage-o''hara', "
            "'urn:wama:poc:pmu:bay-01:current-l1', "
            "'urn:wama:poc:pmu:bay-01:frequency', "
            "'urn:wama:poc:pmu:bay-01:rocof') "
            'AND "double_value" IS NOT NULL',
        )
        self.assertEqual(
            latest_records_target["builder"]["query"],
            'SELECT "__time", "mrid", "double_value", "timestamp_field", '
            '"timestamp_gateway", "timestamp_mccs", "quality_valid" '
            'FROM "live_measurements" '
            "WHERE \"mrid\" IN ('urn:wama:poc:pmu:bay-01:voltage-o''hara', "
            "'urn:wama:poc:pmu:bay-01:current-l1', "
            "'urn:wama:poc:pmu:bay-01:frequency', "
            "'urn:wama:poc:pmu:bay-01:rocof') "
            'AND "double_value" IS NOT NULL '
            'AND "__time" >= MILLIS_TO_TIMESTAMP(${__from}) '
            'AND "__time" <= MILLIS_TO_TIMESTAMP(${__to}) '
            'ORDER BY "__time" DESC LIMIT 32',
        )

    def test_renders_empty_fleet_and_stable_source_files(self) -> None:
        dashboard = render_fleet_dashboard(())

        self.assertEqual(dashboard["uid"], FLEET_DASHBOARD_UID)
        self.assertEqual(dashboard["links"][0]["title"], "Create measurement session")
        self.assertIn("from=${__from}&to=${__to}", dashboard["links"][0]["url"])
        self.assertNotIn("mrids=", dashboard["links"][0]["url"])
        self.assertIn("No active gateway sources", dashboard["panels"][0]["options"]["content"])
        snapshot = render_snapshot((_source(),))
        self.assertEqual(
            sorted(snapshot),
            ["fleet.json", source_dashboard_filename("pmu-bay-01")],
        )

    def test_fleet_links_follow_source_id_order(self) -> None:
        later = GatewaySource(
            source_id="pmu-bay-02",
            catalog_id="wama-c37-118",
            catalog_revision="def456",
            published_at=datetime(2026, 8, 21, 12, 1, tzinfo=timezone.utc),
            site_id="wama-poc-bay-02",
            display_name="WAMA PoC Bay 02",
            ip_address="192.0.2.11",
            port=4712,
            pmu_idcode=1002,
            signals=_source().signals,
        )

        dashboard = render_fleet_dashboard((later, _source()))

        self.assertEqual(
            [link["url"] for link in dashboard["links"]],
            [
                "http://localhost:3004/?from=${__from}&to=${__to}",
                "/d/" + dashboard_uid("pmu-bay-01"),
                "/d/" + dashboard_uid("pmu-bay-02"),
            ],
        )

    def test_source_dashboard_links_to_its_measurement_ids(self) -> None:
        dashboard = render_gateway_dashboard(_source())

        link = dashboard["links"][0]
        self.assertEqual(link["title"], "Create measurement session")
        mrids = parse_qs(urlparse(link["url"]).query)["mrids"][0].split(",")
        self.assertIn("urn:wama:poc:pmu:bay-01:voltage-o'hara", mrids)
        self.assertIn("urn:wama:poc:pmu:bay-01:frequency", mrids)

    def test_publishes_a_snapshot_and_removes_only_stale_gateway_files(self) -> None:
        with TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            store = DashboardStore(directory)
            first_source = _source()
            second_source = GatewaySource(
                source_id="pmu-bay-02",
                catalog_id="wama-c37-118",
                catalog_revision="def456",
                published_at=datetime(2026, 8, 21, 12, 1, tzinfo=timezone.utc),
                site_id="wama-poc-bay-02",
                display_name="WAMA PoC Bay 02",
                ip_address="192.0.2.11",
                port=4712,
                pmu_idcode=1002,
                signals=_source().signals,
            )
            untouched = directory / "other-dashboard.json"
            untouched.write_text("unmanaged", encoding="utf-8")

            filenames = store.publish((first_source, second_source))

            self.assertEqual(
                filenames,
                (
                    "fleet.json",
                    source_dashboard_filename("pmu-bay-01"),
                    source_dashboard_filename("pmu-bay-02"),
                ),
            )
            self.assertEqual(
                json.loads((directory / "fleet.json").read_text(encoding="utf-8"))["uid"],
                FLEET_DASHBOARD_UID,
            )

            store.publish((first_source,))

            self.assertTrue((directory / source_dashboard_filename("pmu-bay-01")).is_file())
            self.assertFalse((directory / source_dashboard_filename("pmu-bay-02")).exists())
            self.assertEqual(untouched.read_text(encoding="utf-8"), "unmanaged")

    def test_folds_duplicate_upserts_and_tombstones(self) -> None:
        registry = GatewayRegistry()
        source = _source()

        self.assertTrue(registry.upsert(source))
        self.assertFalse(registry.upsert(source))
        self.assertEqual(registry.sources, (source,))
        self.assertFalse(registry.remove("pmu-bay-02"))
        self.assertTrue(registry.remove(source.source_id))
        self.assertEqual(registry.sources, ())


def _source() -> GatewaySource:
    return GatewaySource(
        source_id="pmu-bay-01",
        catalog_id="wama-c37-118",
        catalog_revision="abc123",
        published_at=datetime(2026, 8, 21, 12, 0, tzinfo=timezone.utc),
        site_id="wama-poc-bay-01",
        display_name="WAMA PoC Bay 01",
        ip_address="192.0.2.10",
        port=4712,
        pmu_idcode=1001,
        signals=(
            GatewaySignal(
                signal_id="voltage-l1",
                source_channel="VL1",
                mrid="urn:wama:poc:pmu:bay-01:voltage-o'hara",
                quantity="voltage",
                unit="V",
            ),
            GatewaySignal(
                signal_id="current-l1",
                source_channel="IL1",
                mrid="urn:wama:poc:pmu:bay-01:current-l1",
                quantity="current",
                unit="A",
            ),
            GatewaySignal(
                signal_id="frequency",
                source_channel="FREQ",
                mrid="urn:wama:poc:pmu:bay-01:frequency",
                quantity="frequency",
                unit="Hz",
            ),
            GatewaySignal(
                signal_id="rocof",
                source_channel="ROCOF",
                mrid="urn:wama:poc:pmu:bay-01:rocof",
                quantity="rocof",
                unit="Hz/s",
            ),
        ),
    )