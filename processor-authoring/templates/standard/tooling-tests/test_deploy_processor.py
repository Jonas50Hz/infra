"""Tests for the standalone processor deployment guard."""

from __future__ import annotations

from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest

from scripts.deploy_processor import (
    DEPLOY_MARKER,
    DeploymentError,
    _validate_deploy_base_root,
    _require_running_state,
    _require_expected_service,
    synchronize_checkout,
)


class DeploymentGuardTests(unittest.TestCase):
    """Keep one processor deployment isolated from other Compose projects."""

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
        with self.assertRaisesRegex(DeploymentError, "processor-frequency-scale"):
            _require_expected_service(["processor-apparent-power"])
        with self.assertRaisesRegex(DeploymentError, "processor-frequency-scale"):
            _require_expected_service([])

    def test_rejects_a_marked_root_outside_the_expected_base_child(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            base = root / "processors"
            expected = base / "processor-frequency-scale"
            unexpected = root / "other" / "processor-frequency-scale"
            base.mkdir(parents=True)
            unexpected.parent.mkdir(parents=True)

            _validate_deploy_base_root(expected, str(base))
            with self.assertRaisesRegex(DeploymentError, "expected child"):
                _validate_deploy_base_root(unexpected, str(base))

    def test_rejects_a_non_running_deployed_container(self) -> None:
        _require_running_state("true")
        with self.assertRaisesRegex(DeploymentError, "not running"):
            _require_running_state("false")

    def _workspace(self, workspace: Path) -> Path:
        workspace.mkdir()
        (workspace / "compose.yaml").write_text("services: {}\n", encoding="utf-8")
        (workspace / "Dockerfile").write_text("FROM scratch\n", encoding="utf-8")
        subprocess.run(["git", "init", "--quiet", str(workspace)], check=True)
        subprocess.run(["git", "-C", str(workspace), "add", "."], check=True)
        return workspace
