#!/usr/bin/env python3
"""Root-owned registry reconciliation for Forgejo-managed WAMA processors."""

from __future__ import annotations

import argparse
from collections.abc import Iterable, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import asdict, dataclass
import fcntl
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys
import tempfile
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import yaml


SCHEMA_VERSION = 1
PROCESSOR_MARKER = ".wama-forgejo-processor-root"
DEPLOY_MANIFEST = ".wama-forgejo-processor-manifest.json"
DEFAULT_PROCESSORS = (
    "processor-frequency-scale",
    "processor-apparent-power",
    "processor-frequency-iec104-export",
    "processor-lfr-frequency-provision",
)
PROCESSOR_NAME = re.compile(r"^processor-[a-z0-9][a-z0-9-]{0,62}$")
SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")


class ProcessorRegistryError(RuntimeError):
    """Raised when the trusted processor registration boundary is unsafe."""


@dataclass(frozen=True)
class Settings:
    """Trusted root-owned paths and credentials for registry operations."""

    admin_email: str
    admin_password: str
    admin_username: str
    api_url: str
    authoring_root: Path
    gateway_deploy_root: Path
    gateway_repository: str
    infra_network: str
    internal_root_url: str
    processor_deploy_base_root: Path
    registry_status_path: Path
    runner_directory: Path
    runner_url: str
    seed_root: Path
    server_config: Path

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> "Settings":
        """Load only root-owned configuration; processors cannot override it."""

        values = os.environ if environment is None else environment
        settings = cls(
            admin_email=_required(values, "FORGEJO_BOOTSTRAP_ADMIN_EMAIL", "wama-admin@local"),
            admin_password=_required(
                values,
                "FORGEJO_BOOTSTRAP_ADMIN_PASSWORD",
                "wama-admin",
            ),
            admin_username=_identifier(
                _required(values, "FORGEJO_BOOTSTRAP_ADMIN_USERNAME", "wama-admin"),
                "FORGEJO_BOOTSTRAP_ADMIN_USERNAME",
            ),
            api_url=_url(values, "FORGEJO_API_URL", "http://forgejo:3000/api/v1"),
            authoring_root=_absolute_path(
                values,
                "WAMA_PROCESSOR_AUTHORING_ROOT",
                "/opt/wama/processor-authoring",
            ),
            gateway_deploy_root=_absolute_path(
                values,
                "WAMA_GATEWAY_C37_118_ONBOARDING_DEPLOY_ROOT",
                "/var/lib/wama-gateway-c37-118-onboarding",
            ),
            gateway_repository=_identifier(
                _required(
                    values,
                    "FORGEJO_GATEWAY_C37_118_ONBOARDING_REPOSITORY",
                    "gateway-c37-118-onboarding",
                ),
                "FORGEJO_GATEWAY_C37_118_ONBOARDING_REPOSITORY",
            ),
            infra_network=_required(values, "WAMA_INFRA_NETWORK", "wama-infra"),
            internal_root_url=_url(
                values,
                "FORGEJO_INTERNAL_ROOT_URL",
                "http://forgejo:3000",
            ),
            processor_deploy_base_root=_absolute_path(
                values,
                "WAMA_PROCESSOR_DEPLOY_BASE_ROOT",
                "/var/lib/wama-processors",
            ),
            registry_status_path=_absolute_path(
                values,
                "WAMA_PROCESSOR_REGISTRY_STATUS_PATH",
                "/registry-status/processors.json",
            ),
            runner_directory=_absolute_path(
                values,
                "FORGEJO_RUNNER_DIRECTORY",
                "/runner",
            ),
            runner_url=_url(
                values,
                "FORGEJO_RUNNER_URL",
                "http://host.docker.internal:3000/",
            ),
            seed_root=_absolute_path(
                values,
                "FORGEJO_PROCESSOR_SEED_ROOT",
                "/opt/wama/seeds",
            ),
            server_config=_absolute_path(
                values,
                "FORGEJO_SERVER_CONFIG",
                "/data/gitea/conf/app.ini",
            ),
        )
        if settings.processor_deploy_base_root == Path("/"):
            raise ProcessorRegistryError("WAMA_PROCESSOR_DEPLOY_BASE_ROOT must not be /")
        return settings


@dataclass(frozen=True)
class ProcessorRegistration:
    """The validated operational record for exactly one processor repository."""

    approval_catalog_id: str
    approval_catalog_revision: str
    catalog_id: str
    catalog_revision: str
    deploy_root: str
    kind: str
    project_name: str
    repository: str
    sdk_sha256: str
    sdk_version: str
    seed: str
    service: str

    @classmethod
    def from_json(cls, raw: object) -> "ProcessorRegistration":
        """Read one persisted entry, rejecting unrecognized or malformed state."""

        if not isinstance(raw, Mapping):
            raise ProcessorRegistryError("registry processor entry must be a mapping")
        expected = {
            "approval_catalog_id",
            "approval_catalog_revision",
            "catalog_id",
            "catalog_revision",
            "deploy_root",
            "kind",
            "project_name",
            "repository",
            "sdk_sha256",
            "sdk_version",
            "seed",
            "service",
        }
        unknown = set(raw).difference(expected)
        missing = expected.difference(raw)
        if unknown or missing:
            raise ProcessorRegistryError("registry processor entry has an invalid schema")
        entry = cls(**{field: _string(raw[field], f"registry.{field}") for field in expected})
        _processor_name(entry.repository, "registry.repository")
        _processor_name(entry.service, "registry.service")
        if entry.service != entry.repository:
            raise ProcessorRegistryError("registry service must equal its repository")
        if entry.seed != entry.repository:
            raise ProcessorRegistryError("registry seed must equal its repository")
        if entry.project_name != f"wama-{entry.repository}":
            raise ProcessorRegistryError("registry project name is not deterministic")
        if entry.kind not in {"formula", "latest-values", "custom"}:
            raise ProcessorRegistryError("registry processor kind is invalid")
        _digest(entry.sdk_sha256, "registry.sdk_sha256")
        _digest(entry.catalog_revision, "registry.catalog_revision")
        _digest(entry.approval_catalog_revision, "registry.approval_catalog_revision")
        if not Path(entry.deploy_root).is_absolute():
            raise ProcessorRegistryError("registry deploy root must be absolute")
        return entry


class ForgejoApi:
    """Small authenticated Forgejo REST client for trusted registry operations."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def ensure_private_repository(self, repository: str) -> None:
        """Create a private empty remote only when it does not already exist."""

        existing = self._get_optional(f"/repos/{self._settings.admin_username}/{repository}")
        if existing is not None:
            if existing.get("private") is not True:
                raise ProcessorRegistryError(
                    f"existing Forgejo repository {repository} must be private"
                )
            return
        created = self._request(
            "POST",
            "/user/repos",
            {"name": repository, "private": True, "auto_init": False},
        )
        if created.get("private") is not True:
            raise ProcessorRegistryError(
                f"Forgejo did not create a private repository for {repository}"
            )

    def delete_named_runners(self, repository: str, names: Iterable[str]) -> None:
        """Remove only explicitly named repository-scoped runner connections."""

        wanted = set(names)
        runners = self._request("GET", f"/repos/{self._settings.admin_username}/{repository}/actions/runners")
        for runner in _items(runners, "runners"):
            if not isinstance(runner, Mapping) or runner.get("name") not in wanted:
                continue
            runner_id = runner.get("id")
            if isinstance(runner_id, bool) or not isinstance(runner_id, int):
                raise ProcessorRegistryError(
                    f"Forgejo runner {runner.get('name')!r} has no numeric id"
                )
            self._request(
                "DELETE",
                f"/repos/{self._settings.admin_username}/{repository}/actions/runners/{runner_id}",
            )

    def assert_no_active_jobs(self, repositories: Iterable[str]) -> None:
        """Fail closed when restarting the single runner could interrupt executing work."""

        non_executing_states = {
            "success",
            "failure",
            "cancelled",
            "skipped",
            "completed",
            "waiting",
            "queued",
            "pending",
        }
        for repository in repositories:
            payload = self._request(
                "GET",
                f"/repos/{self._settings.admin_username}/{repository}/actions/runners/jobs",
            )
            for job in _items(payload, "jobs"):
                if not isinstance(job, Mapping):
                    raise ProcessorRegistryError("Forgejo runner jobs response is malformed")
                state = job.get("status", job.get("state", ""))
                if not isinstance(state, str) or state.lower() not in non_executing_states:
                    raise ProcessorRegistryError(
                        f"cannot change runner configuration while {repository} has an active job"
                    )

    def dispatch_workflow(self, repository: str, workflow: str, ref: str) -> None:
        """Queue an approved repository's existing workflow on one known ref."""

        self._request(
            "POST",
            f"/repos/{self._settings.admin_username}/{repository}/actions/workflows/{workflow}/dispatches",
            {"ref": ref},
        )

    def _get_optional(self, path: str) -> dict[str, object] | None:
        try:
            payload = self._request("GET", path)
        except _ForgejoHttpError as error:
            if error.status == 404:
                return None
            raise ProcessorRegistryError(str(error)) from error
        if not isinstance(payload, dict):
            raise ProcessorRegistryError("Forgejo repository response is malformed")
        return payload

    def _request(self, method: str, path: str, payload: object | None = None) -> object:
        data = None
        headers = {"Accept": "application/json"}
        if payload is not None:
            data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            headers["Content-Type"] = "application/json"
        credentials = f"{self._settings.admin_username}:{self._settings.admin_password}".encode("utf-8")
        import base64

        headers["Authorization"] = "Basic " + base64.b64encode(credentials).decode("ascii")
        request = Request(
            f"{self._settings.api_url.rstrip('/')}{path}",
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urlopen(request, timeout=20) as response:
                body = response.read()
        except HTTPError as error:
            raise _ForgejoHttpError(error.code, f"Forgejo API {method} {path} failed") from error
        except URLError as error:
            raise ProcessorRegistryError(f"Forgejo API {method} {path} is unavailable") from error
        if not body:
            return {}
        try:
            return json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as error:
            raise ProcessorRegistryError(f"Forgejo API {method} {path} returned invalid JSON") from error


@dataclass(frozen=True)
class _ForgejoHttpError(RuntimeError):
    status: int
    message: str

    def __str__(self) -> str:
        return self.message


class ProcessorRegistry:
    """Validate and reconcile processor registrations without auto-discovery."""

    def __init__(self, settings: Settings, api: ForgejoApi | None = None) -> None:
        self.settings = settings
        self.api = api or ForgejoApi(settings)

    @property
    def path(self) -> Path:
        """Return the trusted persistent registry path inside runner storage."""

        return self.settings.runner_directory / "wama-processor-registry.json"

    @property
    def lock_path(self) -> Path:
        """Return the cross-command registry lock path."""

        return self.settings.runner_directory / "wama-processor-registry.lock"

    def bootstrap(self) -> list[ProcessorRegistration]:
        """Initialize approved defaults once, then reconcile only persisted entries."""

        with self.locked():
            registrations = self.load()
            if registrations is None:
                registrations = [self.validate_seed(repository) for repository in DEFAULT_PROCESSORS]
            else:
                registrations = [
                    self.validate_seed(entry.repository) for entry in registrations
                ]
            self._ensure_registered(registrations)
            self._write_runner_config(registrations)
            self.save(registrations)
            return registrations

    def register(self, repository: str, *, restart_runner: bool = True) -> ProcessorRegistration:
        """Approve one validated seed and give it exactly one isolated root."""

        _processor_name(repository, "repository")
        with self.locked():
            registrations = self.load()
            if registrations is None:
                raise ProcessorRegistryError("bootstrap must create the registry before registration")
            existing = next((entry for entry in registrations if entry.repository == repository), None)
            if existing is None:
                entry = self.validate_seed(repository)
                updated = [*registrations, entry]
                seeded = self._ensure_registered([entry])[entry.repository]
            else:
                updated = [self.validate_seed(entry.repository) for entry in registrations]
                entry = next(item for item in updated if item.repository == repository)
                self._ensure_registered([entry])
                seeded = True
            self._write_runner_config(updated)
            self.save(updated)
            if restart_runner:
                self._restart_runner(updated)
            if existing is None and not seeded:
                self.api.dispatch_workflow(entry.repository, "processor.yaml", "main")
            return entry

    def deploy_existing(self, repository: str) -> None:
        """Intentionally queue the checked-in main workflow for one active processor."""

        _processor_name(repository, "repository")
        with self.locked():
            registrations = self._required_registry()
            if not any(entry.repository == repository for entry in registrations):
                raise ProcessorRegistryError(f"processor {repository} is not registered")
            self.api.dispatch_workflow(repository, "processor.yaml", "main")

    def unregister(self, repository: str, *, restart_runner: bool = True) -> None:
        """Tear down one marked processor project while retaining its Forgejo remote."""

        _processor_name(repository, "repository")
        with self.locked():
            registrations = self._required_registry()
            entry = next((item for item in registrations if item.repository == repository), None)
            if entry is None:
                raise ProcessorRegistryError(f"processor {repository} is not registered")
            remaining = [item for item in registrations if item.repository != repository]
            self._validate_removal_root(entry)
            self.api.assert_no_active_jobs(
                [*(item.repository for item in registrations), self.settings.gateway_repository]
            )
            self.api.delete_named_runners(entry.repository, self._connection_names(entry.repository))
            self._stop_and_remove_project(entry)
            self._remove_connection_files(entry.repository)
            self._write_runner_config(remaining)
            self.save(remaining)
            if restart_runner:
                self._restart_runner(remaining)

    def status(self) -> dict[str, object]:
        """Return sanitized active registration state without any credentials."""

        with self.locked():
            registrations = self._required_registry()
            return {
                "schema_version": SCHEMA_VERSION,
                "processors": [asdict(entry) for entry in registrations],
            }

    def load(self) -> list[ProcessorRegistration] | None:
        """Load persisted state or return None only before the first bootstrap."""

        if not self.path.exists():
            return None
        if self.path.is_symlink() or not self.path.is_file():
            raise ProcessorRegistryError("processor registry path is not a regular file")
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ProcessorRegistryError("processor registry is unreadable or invalid JSON") from error
        if not isinstance(raw, Mapping) or raw.get("schema_version") != SCHEMA_VERSION:
            raise ProcessorRegistryError("processor registry schema version is unsupported")
        entries = raw.get("processors")
        if not isinstance(entries, list):
            raise ProcessorRegistryError("processor registry processors must be a list")
        registrations = [ProcessorRegistration.from_json(entry) for entry in entries]
        names = [entry.repository for entry in registrations]
        if len(names) != len(set(names)):
            raise ProcessorRegistryError("processor registry repeats a repository")
        for entry in registrations:
            self._validate_entry_root(entry)
        return registrations

    def save(self, registrations: Sequence[ProcessorRegistration]) -> None:
        """Atomically persist validated registry state with owner-only permissions."""

        self.settings.runner_directory.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": SCHEMA_VERSION,
            "processors": [asdict(entry) for entry in registrations],
        }
        _atomic_write(self.path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
        self._write_registry_status(payload)

    def _write_registry_status(self, payload: Mapping[str, object]) -> None:
        status_path = self.settings.registry_status_path
        if status_path.is_symlink():
            raise ProcessorRegistryError("processor registry status path must not be a symbolic link")
        _atomic_write(status_path, json.dumps(payload, indent=2, sort_keys=True) + "\n")
        status_path.chmod(0o644)

    @contextmanager
    def locked(self):
        """Serialize bootstrap and root-admin mutations across one host."""

        self.settings.runner_directory.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

    def validate_seed(self, repository: str) -> ProcessorRegistration:
        """Validate only a direct approved seed and derive every operational field."""

        _processor_name(repository, "repository")
        seed = self._seed_path(repository)
        manifest_path = seed / "processor.yaml"
        compose_path = seed / "compose.yaml"
        for required in (
            manifest_path,
            compose_path,
            seed / "Dockerfile",
            seed / ".forgejo" / "workflows" / "processor.yaml",
            seed / "scripts" / "deploy_processor.py",
        ):
            if required.is_symlink() or not required.is_file():
                raise ProcessorRegistryError(f"processor seed lacks required regular file: {required.name}")
        manifest = _yaml_mapping(manifest_path, "processor manifest")
        if manifest.get("name") != repository:
            raise ProcessorRegistryError("processor manifest name must equal its seed directory")
        if manifest.get("kind") not in {"formula", "latest-values", "custom"}:
            raise ProcessorRegistryError("processor manifest kind is invalid")
        self._validate_authoring_contract(manifest_path)
        compose = _yaml_mapping(compose_path, "processor compose file")
        services = compose.get("services")
        if not isinstance(services, Mapping) or list(services) != [repository]:
            raise ProcessorRegistryError(
                f"processor compose file must contain only {repository}"
            )

        sdk = _mapping(manifest.get("sdk"), "manifest.sdk")
        catalog = _mapping(manifest.get("catalog"), "manifest.catalog")
        approvals = _mapping(manifest.get("approvals"), "manifest.approvals")
        return ProcessorRegistration(
            approval_catalog_id=_string(approvals.get("id"), "manifest.approvals.id"),
            approval_catalog_revision=_digest(
                _string(approvals.get("revision"), "manifest.approvals.revision"),
                "manifest.approvals.revision",
            ),
            catalog_id=_string(catalog.get("id"), "manifest.catalog.id"),
            catalog_revision=_digest(
                _string(catalog.get("revision"), "manifest.catalog.revision"),
                "manifest.catalog.revision",
            ),
            deploy_root=str(self._deploy_root(repository)),
            kind=_string(manifest.get("kind"), "manifest.kind"),
            project_name=f"wama-{repository}",
            repository=repository,
            sdk_sha256=_digest(
                _string(sdk.get("sha256"), "manifest.sdk.sha256"),
                "manifest.sdk.sha256",
            ),
            sdk_version=_string(sdk.get("version"), "manifest.sdk.version"),
            seed=repository,
            service=repository,
        )

    def _validate_authoring_contract(self, manifest_path: Path) -> None:
        authoring_source = self.settings.authoring_root / "src"
        input_catalog = self.settings.authoring_root / "catalog" / "input-catalog.yaml"
        approval_catalog = self.settings.authoring_root / "catalog" / "derived-output-approvals.yaml"
        for required in (authoring_source, input_catalog, approval_catalog):
            if not required.exists() or required.is_symlink():
                raise ProcessorRegistryError("processor authoring evidence is unavailable")
        environment = os.environ.copy()
        pythonpath = environment.get("PYTHONPATH", "")
        environment["PYTHONPATH"] = str(authoring_source) + (
            f":{pythonpath}" if pythonpath else ""
        )
        result = subprocess.run(
            [
                "python3",
                "-m",
                "wama_processor_authoring.cli",
                "validate",
                "--manifest",
                str(manifest_path),
                "--input-catalog",
                str(input_catalog),
                "--approval-catalog",
                str(approval_catalog),
            ],
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            message = result.stderr.strip() or result.stdout.strip() or "unknown validation error"
            raise ProcessorRegistryError(f"processor authoring validation failed: {message}")

    def _ensure_registered(
        self,
        registrations: Iterable[ProcessorRegistration],
    ) -> dict[str, bool]:
        seeded: dict[str, bool] = {}
        for entry in registrations:
            self._validate_entry_root(entry)
            self.api.ensure_private_repository(entry.repository)
            seeded[entry.repository] = self._seed_repository_if_empty(entry)
            self._ensure_deploy_root(entry)
            self._ensure_connection(entry.repository, "ci", self._ci_label())
            self._ensure_connection(entry.repository, "deploy", self._deploy_label())
        return seeded

    def _seed_repository_if_empty(self, entry: ProcessorRegistration) -> bool:
        repository_url = (
            f"{self.settings.internal_root_url.rstrip('/')}/"
            f"{self.settings.admin_username}/{entry.repository}.git"
        )
        header = _basic_auth_header(self.settings.admin_username, self.settings.admin_password)
        result = subprocess.run(
            [
                "git",
                "-c",
                f"http.extraHeader=Authorization: Basic {header}",
                "ls-remote",
                repository_url,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise ProcessorRegistryError(
                f"unable to inspect Forgejo repository {entry.repository} refs"
            )
        if result.stdout.strip():
            return False
        source = self._seed_path(entry.seed)
        with tempfile.TemporaryDirectory(prefix=f"wama-seed-{entry.repository}-") as directory:
            worktree = Path(directory) / entry.repository
            shutil.copytree(source, worktree, symlinks=False)
            commands = (
                ["git", "init", "--initial-branch=main", "--quiet"],
                ["git", "config", "user.name", "WAMA Forgejo bootstrap"],
                ["git", "config", "user.email", self.settings.admin_email],
                ["git", "add", "--all"],
                ["git", "commit", "--quiet", "-m", f"Seed {entry.repository}"],
                [
                    "git",
                    "-c",
                    f"http.extraHeader=Authorization: Basic {header}",
                    "push",
                    repository_url,
                    "HEAD:refs/heads/main",
                ],
            )
            for command in commands:
                result = subprocess.run(command, cwd=worktree, check=False)
                if result.returncode != 0:
                    raise ProcessorRegistryError(
                        f"unable to seed empty Forgejo repository {entry.repository}"
                    )
        return True

    def _ensure_deploy_root(self, entry: ProcessorRegistration) -> None:
        deploy_root = self._entry_deploy_root(entry)
        base_root = self.settings.processor_deploy_base_root
        base_root.mkdir(parents=True, exist_ok=True)
        if base_root.is_symlink() or not base_root.is_dir():
            raise ProcessorRegistryError("processor deployment base root must be a real directory")
        if deploy_root.exists() and (deploy_root.is_symlink() or not deploy_root.is_dir()):
            raise ProcessorRegistryError("processor deployment root must be a real directory")
        deploy_root.mkdir(parents=False, exist_ok=True)
        marker = deploy_root / PROCESSOR_MARKER
        if marker.exists():
            if marker.is_symlink() or not marker.is_file():
                raise ProcessorRegistryError("processor deployment root has an invalid marker")
            return
        if any(deploy_root.iterdir()):
            raise ProcessorRegistryError(
                "processor deployment root must be empty before registry creates its marker"
            )
        marker.write_text(
            f"Managed by the WAMA Forgejo repository {entry.repository}.\n",
            encoding="utf-8",
        )
        marker.chmod(0o600)

    def _ensure_connection(self, repository: str, role: str, label: str) -> None:
        name = self._connection_name(repository, role)
        secret_path = self.settings.runner_directory / f"{name}.secret"
        uuid_path = self.settings.runner_directory / f"{name}.uuid"
        if secret_path.is_symlink() or uuid_path.is_symlink():
            raise ProcessorRegistryError("runner connection credentials must not be symbolic links")
        if secret_path.is_file() and secret_path.stat().st_size and uuid_path.is_file() and uuid_path.stat().st_size:
            return
        command_prefix = [
            "s6-setuidgid",
            "git",
            "forgejo",
            "--config",
            str(self.settings.server_config),
        ]
        secret = subprocess.run(
            [*command_prefix, "forgejo-cli", "actions", "generate-secret"],
            capture_output=True,
            text=True,
            check=False,
        )
        if secret.returncode != 0 or not secret.stdout.strip():
            raise ProcessorRegistryError(f"unable to generate runner secret for {name}")
        _atomic_write(secret_path, secret.stdout.strip())
        secret_path.chmod(0o600)
        _chown_git(secret_path)
        registered = subprocess.run(
            [
                *command_prefix,
                "forgejo-cli",
                "actions",
                "register",
                "--name",
                name,
                "--secret-file",
                str(secret_path),
                "--scope",
                f"{self.settings.admin_username}/{repository}",
                "--labels",
                label,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        uuid = registered.stdout.strip()
        if registered.returncode != 0 or not uuid:
            detail = registered.stderr.strip() or registered.stdout.strip() or "no diagnostic"
            raise ProcessorRegistryError(
                f"unable to register runner connection {name}: {detail}"
            )
        _atomic_write(uuid_path, uuid)
        uuid_path.chmod(0o600)
        _chown_git(uuid_path)

    def _write_runner_config(self, registrations: Sequence[ProcessorRegistration]) -> None:
        package_token_path = self.settings.runner_directory / "forgejo-processors-package.token"
        if package_token_path.is_symlink() or not package_token_path.is_file():
            raise ProcessorRegistryError("Forgejo package token is unavailable")
        package_token = package_token_path.read_text(encoding="utf-8").strip()
        if not package_token:
            raise ProcessorRegistryError("Forgejo package token is empty")
        roots = [self._entry_deploy_root(entry) for entry in registrations]
        roots.append(self.settings.gateway_deploy_root)
        connections: list[tuple[str, str, str]] = []
        for entry in registrations:
            for role in ("ci", "deploy"):
                connections.append(
                    (
                        self._connection_name(entry.repository, role),
                        self._connection_file(entry.repository, role, "uuid"),
                        self._connection_file(entry.repository, role, "secret"),
                    )
                )
        for role in ("ci", "deploy"):
            name = self._gateway_connection_name(role)
            uuid = self.settings.runner_directory / f"{name}.uuid"
            secret = self.settings.runner_directory / f"{name}.secret"
            if uuid.is_file() and secret.is_file() and not uuid.is_symlink() and not secret.is_symlink():
                connections.append((name, uuid, secret))
        if not connections:
            raise ProcessorRegistryError("runner configuration has no scoped connections")

        lines = [
            "log:",
            "  level: info",
            "  job_level: info",
            "runner:",
            "  file: /data/.runner",
            "  capacity: 1",
            "  labels:",
            f"    - {_yaml_string(self._ci_label())}",
            f"    - {_yaml_string(self._deploy_label())}",
            "  envs:",
            f"    WAMA_INFRA_NETWORK: {_yaml_string(self.settings.infra_network)}",
            f"    FORGEJO_PROCESSORS_PACKAGE_USERNAME: {_yaml_string(self.settings.admin_username)}",
            f"    FORGEJO_PROCESSORS_PACKAGE_TOKEN: {_yaml_string(package_token)}",
            "container:",
            "  docker_host: automount",
            "  valid_volumes:",
        ]
        for root in roots:
            lines.append(f"    - {_yaml_string(str(root))}")
        lines.extend(
            [
                '  options: "--add-host=host.docker.internal:host-gateway --cpus=2 --memory=2g"',
                "server:",
                "  connections:",
            ]
        )
        for name, uuid_path, secret_path in connections:
            uuid = _credential_file(uuid_path)
            secret = _credential_file(secret_path)
            lines.extend(
                [
                    f"    {name}:",
                    f"      url: {_yaml_string(self.settings.runner_url)}",
                    f"      uuid: {_yaml_string(uuid)}",
                    f"      token: {_yaml_string(secret)}",
                ]
            )
        _atomic_write(self.settings.runner_directory / "config.yaml", "\n".join(lines) + "\n")

    def _restart_runner(self, registrations: Sequence[ProcessorRegistration]) -> None:
        self.api.assert_no_active_jobs(
            [*(entry.repository for entry in registrations), self.settings.gateway_repository]
        )
        project = os.environ.get("COMPOSE_PROJECT_NAME", "infra")
        result = subprocess.run(
            [
                "docker",
                "ps",
                "--quiet",
                "--filter",
                f"label=com.docker.compose.project={project}",
                "--filter",
                "label=com.docker.compose.service=forgejo-runner",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            raise ProcessorRegistryError("unable to inspect the Forgejo runner container")
        container_ids = [line for line in result.stdout.splitlines() if line]
        if not container_ids:
            return
        if len(container_ids) != 1:
            raise ProcessorRegistryError("expected exactly one Forgejo runner container")
        restarted = subprocess.run(
            ["docker", "restart", container_ids[0]],
            check=False,
            capture_output=True,
            text=True,
        )
        if restarted.returncode != 0:
            raise ProcessorRegistryError("unable to restart the Forgejo runner container")

    def _validate_removal_root(self, entry: ProcessorRegistration) -> None:
        root = self._entry_deploy_root(entry)
        marker = root / PROCESSOR_MARKER
        if root.is_symlink() or not root.is_dir() or marker.is_symlink() or not marker.is_file():
            raise ProcessorRegistryError("processor deployment root is not safely marker-owned")
        manifest_path = root / DEPLOY_MANIFEST
        managed_files: set[PurePosixPath] = set()
        if manifest_path.exists():
            if manifest_path.is_symlink() or not manifest_path.is_file():
                raise ProcessorRegistryError("processor deployment manifest is unsafe")
            try:
                deployment_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as error:
                raise ProcessorRegistryError("processor deployment manifest is invalid") from error
            files = deployment_manifest.get("files")
            if not isinstance(files, list) or not all(isinstance(item, str) for item in files):
                raise ProcessorRegistryError("processor deployment manifest has invalid files")
            managed_files = {_managed_relative_path(item) for item in files}
        for path in root.rglob("*"):
            if path.is_symlink():
                raise ProcessorRegistryError("processor deployment root contains a symbolic link")
            relative = PurePosixPath(path.relative_to(root).as_posix())
            if path.is_file():
                if relative in {PurePosixPath(PROCESSOR_MARKER), PurePosixPath(DEPLOY_MANIFEST)}:
                    continue
                if relative not in managed_files:
                    raise ProcessorRegistryError(
                        f"processor deployment root contains unmanaged file {relative}"
                    )
            elif path.is_dir():
                if not any(managed_file.is_relative_to(relative) for managed_file in managed_files):
                    raise ProcessorRegistryError(
                        f"processor deployment root contains unmanaged directory {relative}"
                    )
            else:
                raise ProcessorRegistryError("processor deployment root contains an unsafe path")

    def _stop_and_remove_project(self, entry: ProcessorRegistration) -> None:
        root = self._entry_deploy_root(entry)
        compose_path = root / "compose.yaml"
        if compose_path.exists():
            if compose_path.is_symlink() or not compose_path.is_file():
                raise ProcessorRegistryError("processor compose file is unsafe")
            compose = _yaml_mapping(compose_path, "deployed processor compose file")
            services = compose.get("services")
            if not isinstance(services, Mapping) or list(services) != [entry.service]:
                raise ProcessorRegistryError(
                    f"deployed processor compose file must contain only {entry.service}"
                )
            environment = os.environ.copy()
            environment.update(
                {
                    "WAMA_INFRA_NETWORK": self.settings.infra_network,
                    "WAMA_PROCESSOR_COMPOSE_PROJECT_NAME": entry.project_name,
                }
            )
            stopped = subprocess.run(
                [
                    "docker",
                    "compose",
                    "--project-name",
                    entry.project_name,
                    "--file",
                    str(compose_path),
                    "down",
                    "--remove-orphans",
                    "--volumes",
                ],
                cwd=root,
                env=environment,
                check=False,
                capture_output=True,
                text=True,
            )
            if stopped.returncode != 0:
                raise ProcessorRegistryError(
                    f"unable to stop processor project {entry.project_name}"
                )
        shutil.rmtree(root)

    def _remove_connection_files(self, repository: str) -> None:
        for role in ("ci", "deploy"):
            for suffix in ("secret", "uuid"):
                path = self._connection_file(repository, role, suffix)
                if path.is_symlink():
                    raise ProcessorRegistryError("runner connection credential is a symbolic link")
                path.unlink(missing_ok=True)

    def _required_registry(self) -> list[ProcessorRegistration]:
        registrations = self.load()
        if registrations is None:
            raise ProcessorRegistryError("processor registry has not been bootstrapped")
        return registrations

    def _seed_path(self, repository: str) -> Path:
        root = self.settings.seed_root.resolve()
        candidate = self.settings.seed_root / repository
        if candidate.is_symlink() or not candidate.is_dir():
            raise ProcessorRegistryError("processor seed must be a direct real directory")
        resolved = candidate.resolve()
        if resolved.parent != root:
            raise ProcessorRegistryError("processor seed must be a direct child of the seed root")
        return resolved

    def _deploy_root(self, repository: str) -> Path:
        return self.settings.processor_deploy_base_root / repository

    def _entry_deploy_root(self, entry: ProcessorRegistration) -> Path:
        configured = Path(entry.deploy_root)
        expected = self._deploy_root(entry.repository)
        if configured != expected:
            raise ProcessorRegistryError("processor registry deploy root is not deterministic")
        try:
            configured.relative_to(self.settings.processor_deploy_base_root)
        except ValueError as error:
            raise ProcessorRegistryError("processor deploy root escapes its base root") from error
        return configured

    def _validate_entry_root(self, entry: ProcessorRegistration) -> None:
        self._entry_deploy_root(entry)
        if entry.seed != entry.repository or entry.service != entry.repository:
            raise ProcessorRegistryError("processor registry entry is not one-service owned")

    def _connection_name(self, repository: str, role: str) -> str:
        return f"wama-{repository}-{role}"

    def _connection_names(self, repository: str) -> tuple[str, str]:
        return (
            self._connection_name(repository, "ci"),
            self._connection_name(repository, "deploy"),
        )

    def _connection_file(self, repository: str, role: str, suffix: str) -> Path:
        return self.settings.runner_directory / f"{self._connection_name(repository, role)}.{suffix}"

    def _gateway_connection_name(self, role: str) -> str:
        return f"wama-{self.settings.gateway_repository}-{role}"

    @staticmethod
    def _ci_label() -> str:
        return "wama-processors-ci:docker://wama-forgejo-runner:local"

    @staticmethod
    def _deploy_label() -> str:
        return "wama-processors-deploy:host"


def main(arguments: Sequence[str] | None = None) -> int:
    """Run the trusted bootstrap or root-admin registry command."""

    parser = argparse.ArgumentParser(description="Manage approved WAMA processors")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("bootstrap")
    register = commands.add_parser("register")
    register.add_argument("repository")
    deploy_existing = commands.add_parser("deploy-existing")
    deploy_existing.add_argument("repository")
    unregister = commands.add_parser("unregister")
    unregister.add_argument("repository")
    commands.add_parser("status")
    parser.add_argument("--no-restart-runner", action="store_true")
    args = parser.parse_args(arguments)
    try:
        registry = ProcessorRegistry(Settings.from_environment())
        if args.command == "bootstrap":
            registry.bootstrap()
            return 0
        if args.command == "register":
            entry = registry.register(
                args.repository,
                restart_runner=not args.no_restart_runner,
            )
            print(json.dumps(asdict(entry), sort_keys=True))
            return 0
        if args.command == "deploy-existing":
            registry.deploy_existing(args.repository)
            return 0
        if args.command == "unregister":
            registry.unregister(
                args.repository,
                restart_runner=not args.no_restart_runner,
            )
            return 0
        print(json.dumps(registry.status(), indent=2, sort_keys=True))
        return 0
    except ProcessorRegistryError as error:
        print(f"Processor registry failed: {error}", file=sys.stderr)
        return 1


def _atomic_write(path: Path, contents: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=path.parent,
        prefix=f".{path.name}.",
        delete=False,
    ) as temporary:
        temporary.write(contents)
        temporary.flush()
        os.fsync(temporary.fileno())
        temporary_path = Path(temporary.name)
    temporary_path.chmod(0o600)
    os.replace(temporary_path, path)


def _items(payload: object, field: str) -> list[object]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, Mapping):
        items = payload.get(field, payload.get("data", []))
        if isinstance(items, list):
            return items
    raise ProcessorRegistryError(f"Forgejo {field} response is malformed")


def _managed_relative_path(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise ProcessorRegistryError("processor deployment manifest contains an unsafe path")
    return path


def _credential_file(path: Path) -> str:
    if path.is_symlink() or not path.is_file():
        raise ProcessorRegistryError(f"runner connection credential is missing: {path.name}")
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise ProcessorRegistryError(f"runner connection credential is empty: {path.name}")
    return value


def _yaml_mapping(path: Path, label: str) -> Mapping[str, object]:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as error:
        raise ProcessorRegistryError(f"unable to read {label}") from error
    return _mapping(raw, label)


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ProcessorRegistryError(f"{label} must be a mapping")
    return value


def _required(values: Mapping[str, str], name: str, default: str) -> str:
    return _string(values.get(name, default), name)


def _string(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ProcessorRegistryError(f"{label} must be a non-empty string")
    return value.strip()


def _identifier(value: str, label: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9._-]+", value):
        raise ProcessorRegistryError(f"{label} contains unsafe characters")
    return value


def _processor_name(value: str, label: str) -> str:
    if not PROCESSOR_NAME.fullmatch(value):
        raise ProcessorRegistryError(f"{label} must be a processor-* identifier")
    return value


def _digest(value: str, label: str) -> str:
    if not SHA256.fullmatch(value):
        raise ProcessorRegistryError(f"{label} must be a lowercase sha256 digest")
    return value


def _absolute_path(values: Mapping[str, str], name: str, default: str) -> Path:
    value = Path(_required(values, name, default))
    if not value.is_absolute():
        raise ProcessorRegistryError(f"{name} must be an absolute path")
    return value


def _url(values: Mapping[str, str], name: str, default: str) -> str:
    value = _required(values, name, default)
    if not value.startswith(("http://", "https://")):
        raise ProcessorRegistryError(f"{name} must be an HTTP(S) URL")
    return value


def _basic_auth_header(username: str, password: str) -> str:
    import base64

    return base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")


def _yaml_string(value: str) -> str:
    return json.dumps(value, ensure_ascii=True)


def _chown_git(path: Path) -> None:
    try:
        shutil.chown(path, user="git", group="git")
    except (LookupError, OSError) as error:
        raise ProcessorRegistryError(
            f"unable to assign runner credential ownership to git: {path.name}"
        ) from error


if __name__ == "__main__":
    raise SystemExit(main())