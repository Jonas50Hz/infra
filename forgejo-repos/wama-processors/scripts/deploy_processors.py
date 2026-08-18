"""Deploy only application-repository processor containers."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

DEPLOY_MARKER = ".wama-forgejo-applications-root"
DEPLOY_MANIFEST = ".wama-forgejo-applications-manifest.json"


class DeploymentError(RuntimeError):
    """Raised when an application deployment target is unsafe or invalid."""


def synchronize_application_checkout(workspace: Path, deploy_root: Path, commit: str) -> None:
    """Copy only tracked application files into the marked application deploy root."""

    workspace = workspace.resolve()
    deploy_root = _validate_deploy_root(workspace, deploy_root)
    _validate_application_root(workspace)
    tracked_files = _tracked_files(workspace)
    current_files = {path.as_posix() for path in tracked_files}
    for stale_path in _previous_manifest_files(deploy_root).difference(current_files):
        _remove_managed_file(deploy_root, Path(stale_path))
    for relative_path in tracked_files:
        source = workspace / relative_path
        destination = deploy_root / relative_path
        if source.is_symlink():
            raise DeploymentError(f"Tracked symbolic links are not supported: {relative_path}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    (deploy_root / DEPLOY_MANIFEST).write_text(
        json.dumps({"commit": commit, "files": sorted(current_files)}, indent=2) + "\n",
        encoding="utf-8",
    )


def deploy_processors(
    deploy_root: Path,
    image_prefix: str,
    commit: str,
    project_name: str,
    infra_network: str,
) -> None:
    """Pull and recreate processor services on the external infrastructure network."""

    subprocess.run(["docker", "network", "inspect", infra_network], check=True)
    environment = os.environ.copy()
    environment["WAMA_APPLICATION_IMAGE_PREFIX"] = image_prefix
    environment["WAMA_APPLICATION_IMAGE_TAG"] = "main"
    environment["WAMA_APPS_COMPOSE_PROJECT_NAME"] = project_name
    environment["WAMA_INFRA_NETWORK"] = infra_network
    compose_command = ["docker", "compose", "--project-name", project_name, "-f", "compose.yaml"]
    _run_compose(compose_command, deploy_root, environment, "config", "--quiet")
    processors = [
        service
        for service in _compose_services(compose_command, deploy_root, environment)
        if service.startswith("processor-")
    ]
    if not processors:
        raise DeploymentError("No application processor services were found")
    _run_compose(compose_command, deploy_root, environment, "pull", *processors)
    _run_compose(compose_command, deploy_root, environment, "up", "-d", "--remove-orphans", *processors)
    _verify_revisions(compose_command, deploy_root, environment, processors, commit)


def _validate_application_root(workspace: Path) -> None:
    if not (workspace / "compose.yaml").is_file() or not (workspace / "processors").is_dir():
        raise DeploymentError("Forgejo checkout is not a WAMA application repository")
    if (workspace / "docker-compose.yml").exists():
        raise DeploymentError("Application deployment must not use an infrastructure checkout")


def _validate_deploy_root(workspace: Path, deploy_root: Path) -> Path:
    if not deploy_root.is_absolute():
        raise DeploymentError("WAMA_APPS_DEPLOY_ROOT must be an absolute path")
    deploy_root = deploy_root.resolve()
    if deploy_root == Path("/"):
        raise DeploymentError("WAMA_APPS_DEPLOY_ROOT must not be /")
    if deploy_root == workspace or deploy_root.is_relative_to(workspace):
        raise DeploymentError("WAMA_APPS_DEPLOY_ROOT must not be inside the Forgejo workspace")
    if not (deploy_root / DEPLOY_MARKER).is_file():
        raise DeploymentError(f"Application deploy root lacks {DEPLOY_MARKER}")
    return deploy_root


def _tracked_files(workspace: Path) -> list[Path]:
    result = subprocess.run(
        ["git", "-C", str(workspace), "ls-files", "-z"],
        check=True,
        capture_output=True,
    )
    return [Path(entry.decode("utf-8")) for entry in result.stdout.split(b"\0") if entry]


def _previous_manifest_files(deploy_root: Path) -> set[str]:
    manifest_path = deploy_root / DEPLOY_MANIFEST
    if not manifest_path.is_file():
        return set()
    try:
        contents = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise DeploymentError(f"Invalid application deployment manifest: {manifest_path}") from error
    files = contents.get("files")
    if not isinstance(files, list) or not all(isinstance(item, str) for item in files):
        raise DeploymentError(f"Invalid application deployment manifest: {manifest_path}")
    return set(files)


def _remove_managed_file(deploy_root: Path, relative_path: Path) -> None:
    target = deploy_root / relative_path
    if target.is_file() or target.is_symlink():
        target.unlink()


def _run_compose(
    compose_command: list[str],
    deploy_root: Path,
    environment: dict[str, str],
    *arguments: str,
) -> None:
    subprocess.run([*compose_command, *arguments], cwd=deploy_root, env=environment, check=True)


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


def _verify_revisions(
    compose_command: list[str],
    deploy_root: Path,
    environment: dict[str, str],
    processors: list[str],
    commit: str,
) -> None:
    for processor in processors:
        container_id = subprocess.run(
            [*compose_command, "ps", "--quiet", processor],
            cwd=deploy_root,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if not container_id:
            raise DeploymentError(f"Deployment did not create {processor}")
        revision = subprocess.run(
            ["docker", "inspect", "--format", '{{ index .Config.Labels "org.opencontainers.image.revision" }}', container_id],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if revision != commit:
            raise DeploymentError(f"{processor} revision {revision or '<missing>'} does not match {commit}")


def main() -> int:
    """Synchronize the Forgejo application checkout and deploy processors only."""

    parser = argparse.ArgumentParser(description="Deploy WAMA application processors")
    parser.add_argument("--commit", required=True)
    parser.add_argument("--image-prefix", required=True)
    parser.add_argument(
        "--project-name",
        default=os.environ.get("WAMA_APPS_COMPOSE_PROJECT_NAME", "wama-applications"),
    )
    arguments = parser.parse_args()
    deploy_root_value = os.environ.get("WAMA_APPS_DEPLOY_ROOT")
    if not deploy_root_value:
        print("Deployment failed: WAMA_APPS_DEPLOY_ROOT must be configured", file=sys.stderr)
        return 1
    try:
        workspace = Path(os.environ.get("FORGEJO_WORKSPACE", Path.cwd()))
        deploy_root = Path(deploy_root_value)
        synchronize_application_checkout(workspace, deploy_root, arguments.commit)
        deploy_processors(
            deploy_root,
            arguments.image_prefix,
            arguments.commit,
            arguments.project_name,
            os.environ.get("WAMA_INFRA_NETWORK", "wama-infra"),
        )
    except (DeploymentError, OSError, subprocess.CalledProcessError) as error:
        print(f"Deployment failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())