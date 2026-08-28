"""Tests for the standalone frequency measurement-session deployment guard."""

from __future__ import annotations

from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest

from scripts.deploy_processor import (
    DEPLOY_MARKER,
    DeploymentError,
    _require_expected_service,
    synchronize_checkout,
)


class DeploymentGuardTests(unittest.TestCase):
    """Keep deployment isolated from other Compose projects and services."""

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

    def test_rejects_other_or_missing_compose_services(self) -> None:
        with self.assertRaisesRegex(DeploymentError, "processor-frequency-measurement-session"):
            _require_expected_service(["processor-frequency-scale"])
        with self.assertRaisesRegex(DeploymentError, "processor-frequency-measurement-session"):
            _require_expected_service([])

    def _workspace(self, workspace: Path) -> Path:
        workspace.mkdir()
        (workspace / "compose.yaml").write_text("services: {}\n", encoding="utf-8")
        (workspace / "Dockerfile").write_text("FROM scratch\n", encoding="utf-8")
        subprocess.run(["git", "init", "--quiet", str(workspace)], check=True)
        subprocess.run(["git", "-C", str(workspace), "add", "."], check=True)
        return workspace