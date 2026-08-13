"""Tests for deployment checkout synchronization safeguards."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import subprocess
import unittest

from scripts.deploy_services import DEPLOY_MARKER, DeploymentError, synchronize_checkout


class DeploymentSynchronizationTests(unittest.TestCase):
    """Exercise deployment-root changes without invoking Docker Compose."""

    def test_synchronizes_tracked_files_and_preserves_local_environment(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = self._create_workspace(root / "workspace")
            deploy_root = root / "deploy"
            deploy_root.mkdir()
            (deploy_root / DEPLOY_MARKER).touch()
            (deploy_root / ".env").write_text("LOCAL_SECRET=preserved\n", encoding="utf-8")
            stale_file = deploy_root / "removed.txt"
            stale_file.write_text("stale\n", encoding="utf-8")
            (deploy_root / ".wama-deploy-manifest.json").write_text(
                '{"files": ["removed.txt"]}\n',
                encoding="utf-8",
            )

            synchronized = synchronize_checkout(workspace, deploy_root, "abc123")

            self.assertEqual([path.as_posix() for path in synchronized], ["kept.txt"])
            self.assertEqual((deploy_root / "kept.txt").read_text(encoding="utf-8"), "kept\n")
            self.assertFalse(stale_file.exists())
            self.assertEqual(
                (deploy_root / ".env").read_text(encoding="utf-8"),
                "LOCAL_SECRET=preserved\n",
            )

    def test_rejects_unmarked_deployment_root(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            workspace = self._create_workspace(root / "workspace")
            deploy_root = root / "deploy"
            deploy_root.mkdir()

            with self.assertRaises(DeploymentError):
                synchronize_checkout(workspace, deploy_root, "abc123")

    def _create_workspace(self, workspace: Path) -> Path:
        workspace.mkdir()
        (workspace / "kept.txt").write_text("kept\n", encoding="utf-8")
        subprocess.run(["git", "init", "--quiet", str(workspace)], check=True)
        subprocess.run(["git", "-C", str(workspace), "add", "kept.txt"], check=True)
        return workspace