"""Tests for deterministic gateway Grafana dashboard rendering."""

from __future__ import annotations

from datetime import datetime, timezone
import unittest

from gateway_dashboard_provisioner.model import GatewaySignal, GatewaySource
from gateway_dashboard_provisioner.render import (
    FLEET_DASHBOARD_UID,
    dashboard_uid,
    render_fleet_dashboard,
    render_gateway_dashboard,
    render_snapshot,
    source_dashboard_filename,
)


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

    def test_renders_empty_fleet_and_stable_source_files(self) -> None:
        dashboard = render_fleet_dashboard(())

        self.assertEqual(dashboard["uid"], FLEET_DASHBOARD_UID)
        self.assertEqual(dashboard["links"], [])
        self.assertIn("No active gateway sources", dashboard["panels"][0]["options"]["content"])
        snapshot = render_snapshot((_source(),))
        self.assertEqual(
            sorted(snapshot),
            ["fleet.json", source_dashboard_filename("pmu-bay-01")],
        )

    def test_fleet_links_follow_source_id_order(self) -> None:
        later = GatewaySource(
            source_id="pmu-bay-02",
            catalog_id="wama-c37-118-onboarding",
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
            ["/d/" + dashboard_uid("pmu-bay-01"), "/d/" + dashboard_uid("pmu-bay-02")],
        )


def _source() -> GatewaySource:
    return GatewaySource(
        source_id="pmu-bay-01",
        catalog_id="wama-c37-118-onboarding",
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