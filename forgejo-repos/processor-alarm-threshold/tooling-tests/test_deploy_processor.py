"""Tests for the standalone alarm threshold deployment guard."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from scripts.deploy_processor import (
    DEPLOY_MARKER,
    EXPECTED_SERVICE,
    DeploymentError,
    LEGACY_ACTIVE_ALARM_BOOTSTRAP_ENVIRONMENT,
    LEGACY_ACTIVE_ALARM_BOOTSTRAP_VALUE,
    _compose_environment,
    _require_expected_service,
    deploy_processor,
    main,
    synchronize_checkout,
)


class DeploymentGuardTests(unittest.TestCase):
    """Keep this direct alarm processor deployment isolated from infrastructure."""

    def test_default_compose_environment_scrubs_inherited_migration_value(self) -> None:
        with patch.dict(
            os.environ,
            {LEGACY_ACTIVE_ALARM_BOOTSTRAP_ENVIRONMENT: "accidentally-inherited"},
        ):
            environment = _compose_environment(
                "registry.example/processor:main",
                "wama-processor-alarm-threshold",
                "wama-infra",
                allow_legacy_active_alarm_bootstrap=False,
            )

        self.assertNotIn(LEGACY_ACTIVE_ALARM_BOOTSTRAP_ENVIRONMENT, environment)

    def test_semantic_recovery_flag_injects_exact_migration_value(self) -> None:
        with patch.dict(
            os.environ,
            {LEGACY_ACTIVE_ALARM_BOOTSTRAP_ENVIRONMENT: "accidentally-inherited"},
        ):
            environment = _compose_environment(
                "registry.example/processor:main",
                "wama-processor-alarm-threshold",
                "wama-infra",
                allow_legacy_active_alarm_bootstrap=True,
            )

        self.assertEqual(
            environment[LEGACY_ACTIVE_ALARM_BOOTSTRAP_ENVIRONMENT],
            LEGACY_ACTIVE_ALARM_BOOTSTRAP_VALUE,
        )

    def test_normal_deployment_ignores_deployment_root_env_migration_value(self) -> None:
        compose_calls = self._deploy_with_deployment_root_env(
            allow_legacy_active_alarm_bootstrap=False
        )

        self._assert_compose_uses_empty_env_file(compose_calls)
        for _, environment in compose_calls:
            self.assertNotIn(LEGACY_ACTIVE_ALARM_BOOTSTRAP_ENVIRONMENT, environment)

    def test_recovery_flag_is_the_only_route_to_the_migration_value(self) -> None:
        compose_calls = self._deploy_with_deployment_root_env(
            allow_legacy_active_alarm_bootstrap=True
        )

        self._assert_compose_uses_empty_env_file(compose_calls)
        for _, environment in compose_calls:
            self.assertEqual(
                environment[LEGACY_ACTIVE_ALARM_BOOTSTRAP_ENVIRONMENT],
                LEGACY_ACTIVE_ALARM_BOOTSTRAP_VALUE,
            )

    def test_cli_semantic_flag_enables_recovery_bootstrap(self) -> None:
        with (
            patch.dict(
                os.environ,
                {
                    "FORGEJO_WORKSPACE": "/tmp/forgejo-workspace",
                    "WAMA_PROCESSOR_DEPLOY_ROOT": "/tmp/processor-deploy-root",
                },
                clear=True,
            ),
            patch("scripts.deploy_processor.synchronize_checkout") as synchronize,
            patch("scripts.deploy_processor.deploy_processor") as deploy,
            patch.object(
                sys,
                "argv",
                [
                    "deploy_processor.py",
                    "--commit",
                    "commit-1",
                    "--image",
                    "registry.example/processor:main",
                    "--allow-legacy-active-alarm-bootstrap",
                ],
            ),
        ):
            self.assertEqual(main(), 0)

        synchronize.assert_called_once_with(
            Path("/tmp/forgejo-workspace"),
            Path("/tmp/processor-deploy-root"),
            "commit-1",
        )
        deploy.assert_called_once_with(
            Path("/tmp/processor-deploy-root"),
            "registry.example/processor:main",
            "commit-1",
            "wama-processor-alarm-threshold",
            "wama-infra",
            allow_legacy_active_alarm_bootstrap=True,
        )

    def test_cli_rejects_unrecognized_arguments(self) -> None:
        with patch.object(
            sys,
            "argv",
            [
                "deploy_processor.py",
                "--commit",
                "commit-1",
                "--image",
                "registry.example/processor:main",
                "--raw-migration-token",
                LEGACY_ACTIVE_ALARM_BOOTSTRAP_VALUE,
            ],
        ):
            with self.assertRaises(SystemExit) as error:
                main()

        self.assertEqual(error.exception.code, 2)

    def test_workflow_requires_exact_manual_recovery_confirmation(self) -> None:
        workflow_path = (
            Path(__file__).resolve().parents[1] / ".forgejo" / "workflows" / "processor.yaml"
        )
        if not workflow_path.is_file():
            self.skipTest("Workflow source is not included in the container test image")
        workflow = workflow_path.read_text(encoding="utf-8")

        self.assertIn(
            "  workflow_dispatch:\n"
            "    inputs:\n"
            "      confirm_legacy_active_alarm_recovery:\n",
            workflow,
        )
        self.assertIn("        required: false\n        type: string\n", workflow)
        self.assertIn(
            "          if [[ \"$WAMA_WORKFLOW_EVENT_NAME\" == \"workflow_dispatch\" ]] && \\\n"
            "            [[ \"$WAMA_LEGACY_ACTIVE_ALARM_RECOVERY_CONFIRMATION\" "
            "== \"CONFIRM_LEGACY_ACTIVE_ALARM_RECOVERY\" ]]; then\n"
            "            deployment_arguments+=(--allow-legacy-active-alarm-bootstrap)\n"
            "          fi\n",
            workflow,
        )
        self.assertNotIn(LEGACY_ACTIVE_ALARM_BOOTSTRAP_VALUE, workflow)

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
            self.assertEqual(
                (deploy_root / "config" / "alarm-threshold.yaml").read_text(encoding="utf-8"),
                "version: 1\nrules: []\n",
            )
            self.assertTrue((deploy_root / ".wama-forgejo-processor-manifest.json").is_file())

    def test_rejects_an_unmanaged_destination_file(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = self._workspace(root / "workspace")
            deploy_root = root / "deploy"
            deploy_root.mkdir()
            (deploy_root / DEPLOY_MARKER).write_text("managed\n", encoding="utf-8")
            (deploy_root / "compose.yaml").write_text("unmanaged\n", encoding="utf-8")

            with self.assertRaisesRegex(DeploymentError, "unmanaged"):
                synchronize_checkout(workspace, deploy_root, "commit-1")

    def test_rejects_a_managed_destination_symlink_without_writing_its_target(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = self._workspace(root / "workspace")
            deploy_root = root / "deploy"
            deploy_root.mkdir()
            (deploy_root / DEPLOY_MARKER).write_text("managed\n", encoding="utf-8")
            synchronize_checkout(workspace, deploy_root, "commit-1")
            external_target = root / "external-target"
            external_target.write_text("outside\n", encoding="utf-8")
            destination = deploy_root / "config" / "alarm-threshold.yaml"
            destination.unlink()
            destination.symlink_to(external_target)

            with self.assertRaisesRegex(DeploymentError, "symbolic link"):
                synchronize_checkout(workspace, deploy_root, "commit-2")

            self.assertEqual(external_target.read_text(encoding="utf-8"), "outside\n")

    def test_rejects_other_or_missing_compose_services(self) -> None:
        with self.assertRaisesRegex(DeploymentError, "processor-alarm-threshold"):
            _require_expected_service(["processor-frequency-scale"])
        with self.assertRaisesRegex(DeploymentError, "processor-alarm-threshold"):
            _require_expected_service([])

    def _deploy_with_deployment_root_env(
        self,
        *,
        allow_legacy_active_alarm_bootstrap: bool,
    ) -> list[tuple[list[str], dict[str, str]]]:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = self._workspace(root / "workspace")
            deploy_root = root / "deploy"
            deploy_root.mkdir()
            (deploy_root / DEPLOY_MARKER).write_text("managed\n", encoding="utf-8")
            (deploy_root / ".env").write_text(
                f"{LEGACY_ACTIVE_ALARM_BOOTSTRAP_ENVIRONMENT}="
                f"{LEGACY_ACTIVE_ALARM_BOOTSTRAP_VALUE}\n",
                encoding="utf-8",
            )
            synchronize_checkout(workspace, deploy_root, "commit-1")
            compose_calls: list[tuple[list[str], dict[str, str]]] = []

            def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
                if command[:2] == ["docker", "compose"]:
                    environment = kwargs["env"]
                    self.assertIsInstance(environment, dict)
                    compose_calls.append((command, environment))
                if command[-2:] == ["config", "--services"]:
                    return subprocess.CompletedProcess(
                        command,
                        0,
                        stdout=f"{EXPECTED_SERVICE}\n",
                    )
                if command[-3:] == ["ps", "--quiet", EXPECTED_SERVICE]:
                    return subprocess.CompletedProcess(command, 0, stdout="container-id\n")
                if command[:2] == ["docker", "inspect"]:
                    return subprocess.CompletedProcess(command, 0, stdout="commit-1\n")
                return subprocess.CompletedProcess(command, 0)

            with (
                patch.dict(os.environ, {}, clear=True),
                patch("scripts.deploy_processor.subprocess.run", side_effect=run),
            ):
                deploy_processor(
                    deploy_root,
                    "registry.example/processor:main",
                    "commit-1",
                    "wama-processor-alarm-threshold",
                    "wama-infra",
                    allow_legacy_active_alarm_bootstrap=allow_legacy_active_alarm_bootstrap,
                )

        return compose_calls

    def _assert_compose_uses_empty_env_file(
        self,
        compose_calls: list[tuple[list[str], dict[str, str]]],
    ) -> None:
        self.assertEqual(len(compose_calls), 5)
        for command, _ in compose_calls:
            self.assertEqual(
                command[:4],
                ["docker", "compose", "--env-file", "/dev/null"],
            )

    def _workspace(self, workspace: Path) -> Path:
        workspace.mkdir()
        (workspace / "compose.yaml").write_text("services: {}\n", encoding="utf-8")
        (workspace / "Dockerfile").write_text("FROM scratch\n", encoding="utf-8")
        config_directory = workspace / "config"
        config_directory.mkdir()
        (config_directory / "alarm-threshold.yaml").write_text(
            "version: 1\nrules: []\n",
            encoding="utf-8",
        )
        subprocess.run(["git", "init", "--quiet", str(workspace)], check=True)
        subprocess.run(["git", "-C", str(workspace), "add", "."], check=True)
        return workspace


if __name__ == "__main__":
    unittest.main()