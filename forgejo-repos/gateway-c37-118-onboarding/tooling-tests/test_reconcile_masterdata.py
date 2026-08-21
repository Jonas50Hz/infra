"""Tests for the isolated masterdata-reconciliation deployment guard."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest

from scripts.reconcile_masterdata import (
    DEPLOY_MARKER,
    GATEWAY_COMPOSE_FILE,
    GATEWAY_SERVICE_PREFIX,
    GATEWAY_STATE_FILE,
    DeploymentError,
    _active_source_ids,
    _gateway_service_name,
    _previous_gateway_services,
    _require_expected_service,
    _require_expected_services,
    _require_external_infra_network,
    _write_gateway_compose,
    synchronize_checkout,
)


class DeploymentGuardTests(unittest.TestCase):
    """Ensure a catalog publish cannot turn into infrastructure deployment."""

    def test_synchronizes_tracked_files_to_a_marked_root(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = self._workspace(root / "workspace")
            deploy_root = root / "deploy"
            deploy_root.mkdir()
            (deploy_root / DEPLOY_MARKER).write_text("managed\n", encoding="utf-8")

            synchronize_checkout(workspace, deploy_root, "commit-1")

            self.assertEqual(
                (deploy_root / "compose.yaml").read_text(encoding="utf-8"),
                "services: {}\n",
            )

    def test_rejects_unmanaged_deployment_files(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = self._workspace(root / "workspace")
            deploy_root = root / "deploy"
            deploy_root.mkdir()
            (deploy_root / DEPLOY_MARKER).write_text("managed\n", encoding="utf-8")
            (deploy_root / "compose.yaml").write_text("unmanaged\n", encoding="utf-8")

            with self.assertRaisesRegex(DeploymentError, "unmanaged"):
                synchronize_checkout(workspace, deploy_root, "commit-1")

    def test_rejects_any_service_other_than_the_publisher(self) -> None:
        with self.assertRaisesRegex(DeploymentError, "masterdata-publisher"):
            _require_expected_service(["pmu-gateway"])
        with self.assertRaisesRegex(DeploymentError, "masterdata-publisher"):
            _require_expected_service([])

    def test_allows_only_the_expected_generated_gateway_services(self) -> None:
        gateway_service = _gateway_service_name("pmu-bay-01")
        _require_expected_services(
            ["masterdata-publisher", gateway_service],
            ("masterdata-publisher", gateway_service),
        )
        with self.assertRaisesRegex(DeploymentError, "c37-118-gateway-pmu-bay-01"):
            _require_expected_services(
                ["masterdata-publisher", "pmu-gateway"],
                ("masterdata-publisher", gateway_service),
            )

    def test_derives_safe_gateway_services_from_catalog_filenames(self) -> None:
        with TemporaryDirectory() as directory:
            deploy_root = Path(directory)
            source_directory = deploy_root / "catalog" / "sources"
            source_directory.mkdir(parents=True)
            (source_directory / "pmu-bay-02.yaml").write_text("source_id: pmu-bay-02\n", encoding="utf-8")
            (source_directory / "pmu-bay-01.yaml").write_text("source_id: pmu-bay-01\n", encoding="utf-8")

            source_ids = _active_source_ids(deploy_root)
            _write_gateway_compose(deploy_root, source_ids)
            generated = json.loads((deploy_root / GATEWAY_COMPOSE_FILE).read_text(encoding="utf-8"))

            self.assertEqual(source_ids, ("pmu-bay-01", "pmu-bay-02"))
            self.assertEqual(
                sorted(generated["services"]),
                [
                    f"{GATEWAY_SERVICE_PREFIX}pmu-bay-01",
                    f"{GATEWAY_SERVICE_PREFIX}pmu-bay-02",
                ],
            )
            self.assertEqual(
                generated["services"][f"{GATEWAY_SERVICE_PREFIX}pmu-bay-01"]["networks"],
                ["wama-infra"],
            )

            (source_directory / "unsafe_source.yaml").write_text("source_id: unsafe\n", encoding="utf-8")
            with self.assertRaisesRegex(DeploymentError, "unsafe"):
                _active_source_ids(deploy_root)

    def test_rejects_unsafe_prior_gateway_state(self) -> None:
        with TemporaryDirectory() as directory:
            deploy_root = Path(directory)
            (deploy_root / GATEWAY_STATE_FILE).write_text(
                json.dumps({"services": ["pmu-gateway"]}),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(DeploymentError, "unsafe"):
                _previous_gateway_services(deploy_root)

    def test_requires_the_existing_external_infrastructure_network(self) -> None:
        _require_external_infra_network(
            {"networks": {"wama-infra": {"external": True, "name": "wama-infra"}}},
            "wama-infra",
        )
        with self.assertRaisesRegex(DeploymentError, "external wama-infra"):
            _require_external_infra_network(
                {"networks": {"wama-infra": {"external": False, "name": "wama-infra"}}},
                "wama-infra",
            )

    def _workspace(self, workspace: Path) -> Path:
        workspace.mkdir()
        (workspace / "compose.yaml").write_text("services: {}\n", encoding="utf-8")
        (workspace / "Dockerfile").write_text("FROM scratch\n", encoding="utf-8")
        (workspace / "catalog" / "sources").mkdir(parents=True)
        subprocess.run(["git", "init", "--quiet", str(workspace)], check=True)
        subprocess.run(["git", "-C", str(workspace), "add", "."], check=True)
        return workspace