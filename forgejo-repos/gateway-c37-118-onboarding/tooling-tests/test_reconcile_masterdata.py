"""Tests for the isolated masterdata-reconciliation deployment guard."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from scripts.reconcile_masterdata import (
    DEPLOY_MARKER,
    GATEWAY_COMPOSE_FILE,
    GATEWAY_SERVICE_PREFIX,
    GATEWAY_STATE_FILE,
    PUBLISHER_SERVICE,
    DeploymentError,
    _active_source_ids,
    _gateway_service_name,
    _previous_gateway_services,
    _require_expected_service,
    _require_expected_services,
    _require_external_infra_network,
    _verify_live_measurements,
    _write_gateway_compose,
    reconcile_masterdata,
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
        gateway_services = tuple(
            _gateway_service_name(source_id)
            for source_id in (
                "pmu-bay-01",
                "pmu-bay-02",
                "pmu-bay-03",
                "pmu-bay-04",
                "pmu-bay-05",
            )
        )
        _require_expected_services(
            ["masterdata-publisher", *gateway_services],
            ("masterdata-publisher", *gateway_services),
        )
        with self.assertRaisesRegex(DeploymentError, "c37-118-gateway-pmu-bay-01"):
            _require_expected_services(
                ["masterdata-publisher", *gateway_services, "pmu-gateway"],
                ("masterdata-publisher", *gateway_services),
            )

    def test_derives_safe_gateway_services_from_catalog_filenames(self) -> None:
        with TemporaryDirectory() as directory:
            deploy_root = Path(directory)
            source_directory = deploy_root / "catalog" / "sources"
            source_directory.mkdir(parents=True)
            source_ids = (
                "pmu-bay-01",
                "pmu-bay-02",
                "pmu-bay-03",
                "pmu-bay-04",
                "pmu-bay-05",
            )
            for source_id in reversed(source_ids):
                (source_directory / f"{source_id}.yaml").write_text(
                    f"source_id: {source_id}\n",
                    encoding="utf-8",
                )

            active_source_ids = _active_source_ids(deploy_root)
            _write_gateway_compose(deploy_root, active_source_ids)
            generated = json.loads((deploy_root / GATEWAY_COMPOSE_FILE).read_text(encoding="utf-8"))

            self.assertEqual(active_source_ids, source_ids)
            self.assertEqual(
                sorted(generated["services"]),
                [f"{GATEWAY_SERVICE_PREFIX}{source_id}" for source_id in source_ids],
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

    def test_runs_live_measurement_verification_through_publisher_service(self) -> None:
        compose_command = ["docker", "compose", "-f", "compose.yaml"]
        deploy_root = Path("/tmp/gateway-onboarding-deploy")
        environment = {"WAMA_MASTERDATA_CATALOG_REVISION": "commit-1"}

        with patch("scripts.reconcile_masterdata._run_compose") as run_compose:
            _verify_live_measurements(compose_command, deploy_root, environment)

        run_compose.assert_called_once_with(
            compose_command,
            deploy_root,
            environment,
            "run",
            "--rm",
            "--no-deps",
            PUBLISHER_SERVICE,
            "python",
            "-m",
            "gateway_c37_118_onboarding.verify_live_measurements",
        )

    def test_records_gateway_state_before_live_measurement_verification(self) -> None:
        with TemporaryDirectory() as directory:
            deploy_root = Path(directory)
            observed_state: list[dict[str, object]] = []

            def verify_live_measurements(*_arguments: object) -> None:
                state_path = deploy_root / GATEWAY_STATE_FILE
                self.assertTrue(state_path.is_file())
                observed_state.append(json.loads(state_path.read_text(encoding="utf-8")))

            self._reconcile_one_gateway(deploy_root, verify_live_measurements)

            self.assertEqual(
                observed_state,
                [{"commit": "commit-1", "services": ["c37-118-gateway-pmu-bay-01"]}],
            )

    def test_keeps_gateway_state_when_live_measurement_verification_fails(self) -> None:
        with TemporaryDirectory() as directory:
            deploy_root = Path(directory)

            with self.assertRaises(subprocess.CalledProcessError):
                self._reconcile_one_gateway(
                    deploy_root,
                    subprocess.CalledProcessError(1, "docker compose run"),
                )

            state = json.loads((deploy_root / GATEWAY_STATE_FILE).read_text(encoding="utf-8"))
            self.assertEqual(state["commit"], "commit-1")
            self.assertEqual(state["services"], ["c37-118-gateway-pmu-bay-01"])

    def _reconcile_one_gateway(self, deploy_root: Path, verify_side_effect: object) -> None:
        source_directory = deploy_root / "catalog" / "sources"
        source_directory.mkdir(parents=True)
        (source_directory / "pmu-bay-01.yaml").write_text("source_id: pmu-bay-01\n", encoding="utf-8")
        gateway_service = "c37-118-gateway-pmu-bay-01"
        base_rendered = {
            "networks": {"wama-infra": {"external": True, "name": "wama-infra"}},
        }
        gateway_rendered = {
            **base_rendered,
            "services": {
                PUBLISHER_SERVICE: {"networks": ["wama-infra"]},
                gateway_service: {"networks": ["wama-infra"]},
            },
        }
        with (
            patch("scripts.reconcile_masterdata.subprocess.run"),
            patch("scripts.reconcile_masterdata._run_compose"),
            patch(
                "scripts.reconcile_masterdata._compose_services",
                side_effect=[[PUBLISHER_SERVICE], [PUBLISHER_SERVICE, gateway_service]],
            ),
            patch(
                "scripts.reconcile_masterdata._rendered_compose_config",
                side_effect=[base_rendered, gateway_rendered],
            ),
            patch("scripts.reconcile_masterdata._verify_image_revision"),
            patch("scripts.reconcile_masterdata._verify_gateway_revisions"),
            patch(
                "scripts.reconcile_masterdata._verify_live_measurements",
                side_effect=verify_side_effect,
            ),
        ):
            reconcile_masterdata(
                deploy_root,
                "wama.local/gateway-c37-118-onboarding:main",
                "commit-1",
                "wama-gateway-c37-118-onboarding",
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