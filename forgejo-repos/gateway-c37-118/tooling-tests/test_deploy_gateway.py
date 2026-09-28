"""Tests for the strictly scoped C37.118 gateway deployment guard."""

from __future__ import annotations

from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest

from gateway_c37_118.catalog import Catalog, Signal, Source
from scripts.deploy_gateway import (
    ADAPTER_MANIFEST,
    DEPLOY_MARKER,
    DeploymentError,
    _managed_adapters,
    _remove_adapter,
    _validate_base_compose,
    render_overlay,
    synchronize_checkout,
)


def _catalog(*source_ids: str) -> Catalog:
    sources = tuple(
        Source(
            source_id=source_id,
            site_id=f"site-{index}",
            display_name=f"Demo PMU {index}",
            ip_address="127.0.0.1",
            port=4711 + index,
            pmu_idcode=1000 + index,
            signals=(
                Signal(
                    signal_id="frequency",
                    source_channel="FREQ",
                    mrid=f"urn:wama:demo:pmu:{source_id}:frequency",
                    quantity="frequency",
                    unit="Hz",
                    selector="frequency",
                ),
            ),
        )
        for index, source_id in enumerate(source_ids, start=1)
    )
    return Catalog(catalog_id="demo", sources=sources)


class GatewayDeploymentGuardTests(unittest.TestCase):
    """The generated overlay must never become root Compose control."""

    def test_catalog_membership_adds_and_removes_adapter_services(self) -> None:
        initial_services = render_overlay(
            _catalog("pmu-demo-one"),
            "registry/gateway:main",
        )["services"]
        expanded_services = render_overlay(
            _catalog("pmu-demo-one", "pmu-demo-two"),
            "registry/gateway:main",
        )["services"]
        reduced_services = render_overlay(
            _catalog("pmu-demo-two"),
            "registry/gateway:main",
        )["services"]

        self.assertEqual(set(initial_services), {"c37-118-gateway-pmu-demo-one"})
        self.assertEqual(
            set(expanded_services),
            {"c37-118-gateway-pmu-demo-one", "c37-118-gateway-pmu-demo-two"},
        )
        self.assertEqual(set(reduced_services), {"c37-118-gateway-pmu-demo-two"})
        self.assertEqual(
            set(expanded_services).difference(reduced_services),
            {"c37-118-gateway-pmu-demo-one"},
        )
        services = expanded_services
        self.assertNotIn("pmu-gateway", services)
        for service in services.values():
            self.assertEqual(service["networks"], ["wama-infra"])
            self.assertNotIn("ports", service)

    def test_only_marker_owned_tracked_files_are_synchronized(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = root / "workspace"
            workspace.mkdir()
            (workspace / "compose.yaml").write_text("services: {}\n", encoding="utf-8")
            (workspace / "Dockerfile").write_text("FROM scratch\n", encoding="utf-8")
            subprocess.run(["git", "init", "--quiet", str(workspace)], check=True)
            subprocess.run(["git", "-C", str(workspace), "add", "."], check=True)
            deploy = root / "deploy"
            deploy.mkdir()
            (deploy / DEPLOY_MARKER).write_text("managed\n", encoding="utf-8")
            synchronize_checkout(workspace, deploy, "abc", "registry/gateway:sha-abc")
            self.assertTrue((deploy / "compose.yaml").is_file())
            manifest = (deploy / ".wama-forgejo-gateway-c37-118-manifest.json").read_text(
                encoding="utf-8"
            )
            self.assertIn('"commit": "abc"', manifest)
            self.assertIn('"image": "registry/gateway:sha-abc"', manifest)

    def test_rejects_unmanaged_adapter_name(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / ADAPTER_MANIFEST
            path.write_text('{"services":["pmu-gateway"]}', encoding="utf-8")
            with self.assertRaisesRegex(DeploymentError, "Invalid"):
                _managed_adapters(path)
            with self.assertRaisesRegex(DeploymentError, "non-gateway"):
                _remove_adapter("gateway", "pmu-gateway")

    def test_rejects_extra_base_compose_service(self) -> None:
        with TemporaryDirectory() as directory:
            compose = Path(directory) / "compose.yaml"
            compose.write_text(
                "services: {masterdata-publisher: {}, pmu-gateway: {}}\n"
                "networks: {wama-infra: {external: true}}\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(DeploymentError, "only masterdata"):
                _validate_base_compose(compose)

    def test_rejects_publisher_network_or_port_override(self) -> None:
        with TemporaryDirectory() as directory:
            compose = Path(directory) / "compose.yaml"
            compose.write_text(
                "services:\n"
                "  masterdata-publisher:\n"
                "    networks: [wama-infra]\n"
                "    ports: ['3000:3000']\n"
                "networks: {wama-infra: {external: true}}\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(DeploymentError, "must not expose"):
                _validate_base_compose(compose)


if __name__ == "__main__":
    unittest.main()
