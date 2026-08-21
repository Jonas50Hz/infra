"""Focused safety tests for dynamic Forgejo processor registry state."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "services" / "forgejo-init" / "processor_registry.py"
SPEC = importlib.util.spec_from_file_location("processor_registry", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
registry_module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = registry_module
SPEC.loader.exec_module(registry_module)


class FakeApi:
    """Record only the trusted API operations the registry is allowed to issue."""

    def __init__(self) -> None:
        self.repositories: list[str] = []
        self.deleted: list[tuple[str, tuple[str, ...]]] = []
        self.dispatched: list[tuple[str, str, str]] = []
        self.active_job_checks: list[tuple[str, ...]] = []

    def ensure_private_repository(self, repository: str) -> None:
        self.repositories.append(repository)

    def delete_named_runners(self, repository: str, names: tuple[str, ...]) -> None:
        self.deleted.append((repository, names))

    def assert_no_active_jobs(self, repositories) -> None:
        self.active_job_checks.append(tuple(repositories))

    def dispatch_workflow(self, repository: str, workflow: str, ref: str) -> None:
        self.dispatched.append((repository, workflow, ref))


class ProcessorRegistryTests(unittest.TestCase):
    """Prove registry files cannot expand roots or remove unmanaged content."""

    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        self.seed_root = self.root / "seeds"
        self.seed_root.mkdir()
        self.runner_directory = self.root / "runner"
        self.runner_directory.mkdir()
        self.deploy_base = self.root / "deployments"
        self.authoring_root = ROOT / "processor-authoring"
        self._copy_seed("processor-frequency-scale")
        (self.runner_directory / "forgejo-processors-package.token").write_text(
            "package-token\n",
            encoding="utf-8",
        )
        self._write_connection("wama-gateway-c37-118-onboarding-ci")
        self._write_connection("wama-gateway-c37-118-onboarding-deploy")
        self.settings = registry_module.Settings(
            admin_email="wama-admin@test",
            admin_password="wama-admin",
            admin_username="wama-admin",
            api_url="http://forgejo.test/api/v1",
            authoring_root=self.authoring_root,
            gateway_deploy_root=self.root / "gateway",
            gateway_repository="gateway-c37-118-onboarding",
            infra_network="wama-infra",
            internal_root_url="http://forgejo.test",
            processor_deploy_base_root=self.deploy_base,
            registry_status_path=self.root / "registry-status" / "processors.json",
            runner_directory=self.runner_directory,
            runner_url="http://forgejo.test/",
            seed_root=self.seed_root,
            server_config=self.root / "app.ini",
        )
        self.api = FakeApi()
        self.registry = registry_module.ProcessorRegistry(self.settings, api=self.api)

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def test_validates_a_direct_seed_and_derives_only_its_child_root(self) -> None:
        entry = self.registry.validate_seed("processor-frequency-scale")

        self.assertEqual(entry.service, "processor-frequency-scale")
        self.assertEqual(
            entry.deploy_root,
            str(self.deploy_base / "processor-frequency-scale"),
        )
        self.assertEqual(entry.project_name, "wama-processor-frequency-scale")

    def test_rejects_a_seed_outside_the_direct_seed_root(self) -> None:
        outside = self.root / "processor-outside"
        outside.mkdir()

        with self.assertRaisesRegex(registry_module.ProcessorRegistryError, "direct real directory"):
            self.registry.validate_seed("processor-outside")

    def test_persists_only_validated_registry_entries(self) -> None:
        entry = self.registry.validate_seed("processor-frequency-scale")

        self.registry.save([entry])

        loaded = self.registry.load()
        self.assertEqual(loaded, [entry])
        self.assertEqual((self.runner_directory / "wama-processor-registry.json").stat().st_mode & 0o777, 0o600)
        self.assertEqual(
            json.loads(self.settings.registry_status_path.read_text(encoding="utf-8"))["processors"][0]["repository"],
            "processor-frequency-scale",
        )
        self.assertEqual(self.settings.registry_status_path.stat().st_mode & 0o777, 0o644)

    def test_rejects_non_deterministic_deploy_roots_in_persisted_state(self) -> None:
        entry = self.registry.validate_seed("processor-frequency-scale")
        payload = {"schema_version": 1, "processors": [entry.__dict__ | {"deploy_root": "/tmp/escape"}]}
        (self.runner_directory / "wama-processor-registry.json").write_text(
            json.dumps(payload),
            encoding="utf-8",
        )

        with self.assertRaisesRegex(registry_module.ProcessorRegistryError, "deploy root"):
            self.registry.load()

    def test_refuses_to_remove_unmanaged_deployment_content(self) -> None:
        entry = self.registry.validate_seed("processor-frequency-scale")
        root = Path(entry.deploy_root)
        root.mkdir(parents=True)
        (root / registry_module.PROCESSOR_MARKER).write_text("managed\n", encoding="utf-8")
        (root / "unmanaged.txt").write_text("do not delete\n", encoding="utf-8")

        with self.assertRaisesRegex(registry_module.ProcessorRegistryError, "unmanaged file"):
            self.registry._validate_removal_root(entry)

    def test_accepts_nested_managed_deployment_content(self) -> None:
        entry = self.registry.validate_seed("processor-frequency-scale")
        root = Path(entry.deploy_root)
        nested = root / "nested"
        nested.mkdir(parents=True)
        (root / registry_module.PROCESSOR_MARKER).write_text("managed\n", encoding="utf-8")
        (nested / "file.txt").write_text("managed\n", encoding="utf-8")
        (root / registry_module.DEPLOY_MANIFEST).write_text(
            json.dumps({"commit": "test", "files": ["nested/file.txt"]}),
            encoding="utf-8",
        )

        self.registry._validate_removal_root(entry)

    def test_renders_only_registered_processor_roots_and_gateway_root(self) -> None:
        entry = self.registry.validate_seed("processor-frequency-scale")
        self._write_connection("wama-processor-frequency-scale-ci")
        self._write_connection("wama-processor-frequency-scale-deploy")

        self.registry._write_runner_config([entry])

        config = (self.runner_directory / "config.yaml").read_text(encoding="utf-8")
        self.assertIn(str(self.deploy_base / "processor-frequency-scale"), config)
        self.assertIn(str(self.settings.gateway_deploy_root), config)
        self.assertNotIn(str(self.deploy_base), config.split("valid_volumes:", 1)[1].split("options:", 1)[0].replace(str(self.deploy_base / "processor-frequency-scale"), ""))

    def test_accepts_a_null_forgejo_jobs_response_as_no_jobs(self) -> None:
        self.assertEqual(registry_module._items(None, "jobs"), [])

    def test_treats_waiting_jobs_as_safe_for_runner_activation(self) -> None:
        api = self._jobs_api([{"status": "waiting"}])
        api.assert_no_active_jobs(["processor-frequency-scale"])

    def test_rejects_running_jobs_before_runner_activation(self) -> None:
        api = self._jobs_api([{"status": "running"}])

        with self.assertRaisesRegex(registry_module.ProcessorRegistryError, "active job"):
            api.assert_no_active_jobs(["processor-frequency-scale"])

    def test_dispatches_only_an_active_registered_processor_main_workflow(self) -> None:
        entry = self.registry.validate_seed("processor-frequency-scale")
        self.registry.save([entry])

        self.registry.deploy_existing("processor-frequency-scale")

        self.assertEqual(
            self.api.dispatched,
            [("processor-frequency-scale", "processor.yaml", "main")],
        )
        with self.assertRaisesRegex(registry_module.ProcessorRegistryError, "not registered"):
            self.registry.deploy_existing("processor-not-registered")

    def _copy_seed(self, repository: str) -> None:
        source = ROOT / "forgejo-repos" / repository
        shutil.copytree(source, self.seed_root / repository)

    def _write_connection(self, name: str) -> None:
        (self.runner_directory / f"{name}.secret").write_text("secret\n", encoding="utf-8")
        (self.runner_directory / f"{name}.uuid").write_text("uuid\n", encoding="utf-8")

    def _jobs_api(self, jobs: list[dict[str, str]]):
        class JobsApi(registry_module.ForgejoApi):
            def __init__(self, settings, payload):
                self._settings = settings
                self._payload = payload

            def _request(self, method, path, payload=None):
                return self._payload

        return JobsApi(self.settings, jobs)


if __name__ == "__main__":
    unittest.main()