"""Tests for application deployment checkout synchronization."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import subprocess
import unittest

from scripts.deploy_processors import DEPLOY_MARKER, DeploymentError, synchronize_application_checkout


class DeploymentSynchronizationTests(unittest.TestCase):
    """Ensure application deployment cannot synchronize infrastructure files."""

    def test_synchronizes_tracked_app_files_and_preserves_local_environment(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = self._create_application_workspace(root / "workspace")
            deploy_root = root / "deploy"
            deploy_root.mkdir()
            (deploy_root / DEPLOY_MARKER).touch()
            (deploy_root / ".env").write_text("LOCAL_SECRET=preserved\n", encoding="utf-8")

            synchronize_application_checkout(workspace, deploy_root, "abc123")

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
                synchronize_application_checkout(workspace, deploy_root, "abc123")

    def _create_application_workspace(self, workspace: Path) -> Path:
        workspace.mkdir()
        (workspace / "processors").mkdir()
        (workspace / "compose.yaml").write_text("services: {}\n", encoding="utf-8")
        subprocess.run(["git", "init", "--quiet", str(workspace)], check=True)
        subprocess.run(["git", "-C", str(workspace), "add", "compose.yaml", "processors"], check=True)
        return workspace