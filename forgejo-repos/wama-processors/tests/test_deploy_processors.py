"""Tests for processors deployment checkout synchronization."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import subprocess
import unittest

from scripts.deploy_processors import (
    DEPLOY_MARKER,
    DeploymentError,
    _processor_services,
    synchronize_processors_checkout,
)


class DeploymentSynchronizationTests(unittest.TestCase):
    """Ensure processors deployment cannot synchronize infrastructure files."""

    def test_synchronizes_tracked_app_files_and_preserves_local_environment(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = self._create_application_workspace(root / "workspace")
            deploy_root = root / "deploy"
            deploy_root.mkdir()
            (deploy_root / DEPLOY_MARKER).touch()
            (deploy_root / ".env").write_text("LOCAL_SECRET=preserved\n", encoding="utf-8")

            synchronize_processors_checkout(workspace, deploy_root, "abc123")

            self.assertEqual((deploy_root / "compose.yaml").read_text(encoding="utf-8"), "services: {}\n")
            self.assertEqual(
                (deploy_root / ".env").read_text(encoding="utf-8"),
                "LOCAL_SECRET=preserved\n",
            )

    def test_rejects_infrastructure_style_checkout(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = self._create_application_workspace(root / "workspace")
            (workspace / "docker-compose.yml").write_text("services: {}\n", encoding="utf-8")
            subprocess.run(["git", "-C", str(workspace), "add", "docker-compose.yml"], check=True)
            deploy_root = root / "deploy"
            deploy_root.mkdir()
            (deploy_root / DEPLOY_MARKER).touch()

            with self.assertRaises(DeploymentError):
                synchronize_processors_checkout(workspace, deploy_root, "abc123")

    def test_rejects_unmanaged_file_collision(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = self._create_application_workspace(root / "workspace")
            deploy_root = root / "deploy"
            deploy_root.mkdir()
            (deploy_root / DEPLOY_MARKER).touch()
            (deploy_root / "compose.yaml").write_text(
                "services: {unmanaged: {}}\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(DeploymentError, "unmanaged deployment file"):
                synchronize_processors_checkout(workspace, deploy_root, "abc123")

    def test_rejects_unmarked_nonempty_deploy_root(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = self._create_application_workspace(root / "workspace")
            deploy_root = root / "deploy"
            deploy_root.mkdir()
            (deploy_root / "unmanaged.txt").write_text("retain\n", encoding="utf-8")

            with self.assertRaisesRegex(DeploymentError, "lacks"):
                synchronize_processors_checkout(workspace, deploy_root, "abc123")

    def test_rejects_non_processor_compose_service(self) -> None:
        with self.assertRaisesRegex(DeploymentError, "non-processor"):
            _processor_services(["processor-frequency-scale", "gateway-test"])

    def test_accepts_processor_services_only(self) -> None:
        self.assertEqual(
            _processor_services(["processor-frequency-scale"]),
            ["processor-frequency-scale"],
        )

    def _create_application_workspace(self, workspace: Path) -> Path:
        workspace.mkdir()
        (workspace / "processors").mkdir()
        (workspace / "compose.yaml").write_text("services: {}\n", encoding="utf-8")
        subprocess.run(["git", "init", "--quiet", str(workspace)], check=True)
        subprocess.run(["git", "-C", str(workspace), "add", "compose.yaml", "processors"], check=True)
        return workspace