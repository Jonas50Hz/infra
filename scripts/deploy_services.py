"""Synchronize an approved checkout and deploy WAMA application services."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Iterable

DEPLOY_MARKER = ".wama-deploy-root"
DEPLOY_MANIFEST = ".wama-deploy-manifest.json"


class DeploymentError(RuntimeError):
    """Raised when a deployment target cannot be used safely."""


def synchronize_checkout(workspace: Path, deploy_root: Path, commit: str) -> list[Path]:
    """Copy tracked checkout files while preserving deployment-local configuration."""

    resolved_workspace = workspace.resolve()
    resolved_deploy_root = _validate_deploy_root(resolved_workspace, deploy_root)
    tracked_files = _tracked_files(resolved_workspace)
    previous_files = _previous_manifest_files(resolved_deploy_root)
    current_files = {path.as_posix() for path in tracked_files}

    for stale_file in previous_files.difference(current_files):
        _remove_managed_file(resolved_deploy_root, Path(stale_file))

    for relative_path in tracked_files:
        source = resolved_workspace / relative_path
        destination = resolved_deploy_root / relative_path
        if source.is_symlink():
            raise DeploymentError(f"Tracked symbolic links are not supported: {relative_path}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)

    manifest = {
        "commit": commit,
        "files": sorted(current_files),
    }
    (resolved_deploy_root / DEPLOY_MANIFEST).write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return tracked_files


def deploy_application_services(
    deploy_root: Path,
    image_prefix: str,
    project_name: str,
    commit: str,
) -> None:
    """Pull and recreate application services from the synchronized checkout."""

    environment = os.environ.copy()
    environment["WAMA_IMAGE_PREFIX"] = image_prefix
    environment["WAMA_IMAGE_TAG"] = "main"
    compose_command = ["docker", "compose", "--project-name", project_name]

    _run_compose(compose_command, deploy_root, environment, "config", "--quiet")
    services = _compose_services(compose_command, deploy_root, environment)
    application_services = [
        service
        for service in services
        if service == "pmu-gateway" or service.startswith("processor-")
    ]
    if not application_services:
        raise DeploymentError("No deployable application services were found")

    _run_compose(
        compose_command,
        deploy_root,
        environment,
        "pull",
        *application_services,
    )
    _run_compose(
        compose_command,
        deploy_root,
        environment,
        "up",
        "-d",
        "--remove-orphans",
        *application_services,
    )
    _verify_deployed_revisions(
        compose_command,
        deploy_root,
        environment,
        application_services,
        commit,
    )


def _validate_deploy_root(workspace: Path, deploy_root: Path) -> Path:
    if not deploy_root.is_absolute():
        raise DeploymentError("WAMA_DEPLOY_ROOT must be an absolute path")

    resolved_deploy_root = deploy_root.resolve()
    if resolved_deploy_root == Path("/"):
        raise DeploymentError("WAMA_DEPLOY_ROOT must not be /")
    if resolved_deploy_root == workspace or resolved_deploy_root.is_relative_to(workspace):
        raise DeploymentError("WAMA_DEPLOY_ROOT must not be inside the Actions workspace")
    if not (resolved_deploy_root / DEPLOY_MARKER).is_file():
        raise DeploymentError(
            f"WAMA_DEPLOY_ROOT is missing required marker {DEPLOY_MARKER}"
        )
    return resolved_deploy_root


def _tracked_files(workspace: Path) -> list[Path]:
    result = subprocess.run(
        ["git", "-C", str(workspace), "ls-files", "-z"],
        check=True,
        capture_output=True,
    )
    return [
        Path(entry.decode("utf-8"))
        for entry in result.stdout.split(b"\0")
        if entry
    ]


def _previous_manifest_files(deploy_root: Path) -> set[str]:
    manifest_path = deploy_root / DEPLOY_MANIFEST
    if not manifest_path.is_file():
        return set()

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise DeploymentError(f"Invalid deployment manifest: {manifest_path}") from error

    files = manifest.get("files")
    if not isinstance(files, list) or not all(isinstance(item, str) for item in files):
        raise DeploymentError(f"Invalid deployment manifest file list: {manifest_path}")
    return set(files)


def _remove_managed_file(deploy_root: Path, relative_path: Path) -> None:
    target = deploy_root / relative_path
    if not target.is_file() and not target.is_symlink():
        return
    target.unlink()
    _remove_empty_parent_directories(target.parent, deploy_root)


def _remove_empty_parent_directories(directory: Path, deploy_root: Path) -> None:
    while directory != deploy_root:
        try:
            directory.rmdir()
        except OSError:
            return
        directory = directory.parent


def _run_compose(
    compose_command: list[str],
    deploy_root: Path,
    environment: dict[str, str],
    *arguments: str,
) -> None:
    subprocess.run(
        [*compose_command, *arguments],
        cwd=deploy_root,
        env=environment,
        check=True,
    )


def _compose_services(
    compose_command: list[str],
    deploy_root: Path,
    environment: dict[str, str],
) -> list[str]:
    result = subprocess.run(
        [*compose_command, "config", "--services"],
        cwd=deploy_root,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    return [service for service in result.stdout.splitlines() if service]


def _verify_deployed_revisions(
    compose_command: list[str],
    deploy_root: Path,
    environment: dict[str, str],
    services: Iterable[str],
    commit: str,
) -> None:
    for service in services:
        container_id = subprocess.run(
            [*compose_command, "ps", "--quiet", service],
            cwd=deploy_root,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if not container_id:
            raise DeploymentError(f"Deployment did not create a container for {service}")

        revision = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                '{{ index .Config.Labels "org.opencontainers.image.revision" }}',
                container_id,
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if revision != commit:
            raise DeploymentError(
                f"{service} is running revision {revision or '<missing>'}, expected {commit}"
            )


def main() -> int:
    """Synchronize the Actions checkout and deploy its tested service images."""

    parser = argparse.ArgumentParser(description="Deploy approved WAMA Compose services")
    parser.add_argument("--commit", required=True, help="Commit SHA expected on deployed images")
    parser.add_argument("--image-prefix", required=True, help="Registry/owner/repository prefix")
    parser.add_argument(
        "--project-name",
        default=os.environ.get("WAMA_COMPOSE_PROJECT_NAME", "wama-poc"),
        help="Stable Compose project name for the deployment stack",
    )
    arguments = parser.parse_args()

    workspace = Path(os.environ.get("FORGEJO_WORKSPACE", Path.cwd()))
    deploy_root_value = os.environ.get("WAMA_DEPLOY_ROOT")
    if not deploy_root_value:
        print("Deployment failed: WAMA_DEPLOY_ROOT must be configured", file=sys.stderr)
        return 1

    try:
        deploy_root = Path(deploy_root_value)
        synchronize_checkout(workspace, deploy_root, arguments.commit)
        deploy_application_services(
            deploy_root,
            arguments.image_prefix,
            arguments.project_name,
            arguments.commit,
        )
    except (DeploymentError, OSError, subprocess.CalledProcessError) as error:
        print(f"Deployment failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())