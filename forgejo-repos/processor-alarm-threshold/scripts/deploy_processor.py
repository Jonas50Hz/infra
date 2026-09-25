"""Synchronize and deploy only processor-alarm-threshold."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys


EXPECTED_SERVICE = "processor-alarm-threshold"
DEPLOY_MARKER = ".wama-forgejo-processor-root"
DEPLOY_MANIFEST = ".wama-forgejo-processor-manifest.json"
LEGACY_ACTIVE_ALARM_BOOTSTRAP_ENVIRONMENT = "WAMA_ALARM_EVALUATION_WATERMARK_MIGRATION"
LEGACY_ACTIVE_ALARM_BOOTSTRAP_VALUE = "accept-forward-only-alarm-evaluation-watermark-v1"
EMPTY_COMPOSE_ENV_FILE = "/dev/null"


class DeploymentError(RuntimeError):
    """Raised when this processor deployment target is unsafe or invalid."""


def _compose_environment(
    image: str,
    project_name: str,
    infra_network: str,
    allow_legacy_active_alarm_bootstrap: bool,
) -> dict[str, str]:
    """Build the isolated environment used by every app-local Compose command."""

    environment = os.environ.copy()
    environment.pop(LEGACY_ACTIVE_ALARM_BOOTSTRAP_ENVIRONMENT, None)
    environment["WAMA_PROCESSOR_IMAGE"] = image
    environment["WAMA_PROCESSOR_COMPOSE_PROJECT_NAME"] = project_name
    environment["WAMA_INFRA_NETWORK"] = infra_network
    if allow_legacy_active_alarm_bootstrap:
        environment[LEGACY_ACTIVE_ALARM_BOOTSTRAP_ENVIRONMENT] = (
            LEGACY_ACTIVE_ALARM_BOOTSTRAP_VALUE
        )
    return environment


def synchronize_checkout(workspace: Path, deploy_root: Path, commit: str) -> None:
    """Copy tracked repository files into one marker-owned deployment root."""

    workspace = workspace.resolve()
    deploy_root = _validate_deploy_root(workspace, deploy_root)
    _validate_workspace(workspace)
    tracked_files = _tracked_files(workspace)
    current_files = {path.as_posix() for path in tracked_files}
    previous_files = _previous_manifest_files(deploy_root)
    _validate_copy_destinations(deploy_root, tracked_files, previous_files)
    for stale_path in previous_files.difference(current_files):
        _remove_managed_file(deploy_root, Path(stale_path))
    for relative_path in tracked_files:
        source = workspace / relative_path
        destination = _destination_path(deploy_root, relative_path)
        if source.is_symlink():
            raise DeploymentError(f"Tracked symbolic links are not supported: {relative_path}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, destination)
    manifest_path = _destination_path(deploy_root, Path(DEPLOY_MANIFEST))
    manifest_path.write_text(
        json.dumps({"commit": commit, "files": sorted(current_files)}, indent=2) + "\n",
        encoding="utf-8",
    )


def deploy_processor(
    deploy_root: Path,
    image: str,
    commit: str,
    project_name: str,
    infra_network: str,
    *,
    allow_legacy_active_alarm_bootstrap: bool = False,
) -> None:
    """Pull and recreate exactly one processor on the external infrastructure network."""

    environment = _compose_environment(
        image,
        project_name,
        infra_network,
        allow_legacy_active_alarm_bootstrap,
    )
    subprocess.run(
        ["docker", "network", "inspect", infra_network],
        check=True,
        env=environment,
    )
    compose_command = [
        "docker",
        "compose",
        "--env-file",
        EMPTY_COMPOSE_ENV_FILE,
        "--project-name",
        project_name,
        "-f",
        "compose.yaml",
    ]
    _run_compose(compose_command, deploy_root, environment, "config", "--quiet")
    _require_expected_service(_compose_services(compose_command, deploy_root, environment))
    _run_compose(compose_command, deploy_root, environment, "pull", EXPECTED_SERVICE)
    _run_compose(
        compose_command,
        deploy_root,
        environment,
        "up",
        "-d",
        "--remove-orphans",
        EXPECTED_SERVICE,
    )
    _verify_revision(compose_command, deploy_root, environment, commit)


def _validate_workspace(workspace: Path) -> None:
    if not (workspace / "compose.yaml").is_file() or not (workspace / "Dockerfile").is_file():
        raise DeploymentError("Forgejo checkout is not a processor repository")
    if (workspace / "docker-compose.yml").exists():
        raise DeploymentError("Processor deployment must not use an infrastructure checkout")


def _validate_deploy_root(workspace: Path, deploy_root: Path) -> Path:
    if not deploy_root.is_absolute():
        raise DeploymentError("WAMA_PROCESSOR_DEPLOY_ROOT must be an absolute path")
    if deploy_root.is_symlink():
        raise DeploymentError("WAMA_PROCESSOR_DEPLOY_ROOT must not be a symbolic link")
    deploy_root = deploy_root.resolve()
    if deploy_root == Path("/"):
        raise DeploymentError("WAMA_PROCESSOR_DEPLOY_ROOT must not be /")
    if deploy_root == workspace or deploy_root.is_relative_to(workspace):
        raise DeploymentError("WAMA_PROCESSOR_DEPLOY_ROOT must not be inside the Forgejo workspace")
    marker = deploy_root / DEPLOY_MARKER
    if marker.is_symlink() or not marker.is_file():
        raise DeploymentError(f"Processor deploy root lacks {DEPLOY_MARKER}")
    return deploy_root


def _tracked_files(workspace: Path) -> list[Path]:
    result = subprocess.run(
        ["git", "-C", str(workspace), "ls-files", "-z"],
        check=True,
        capture_output=True,
    )
    return [Path(entry.decode("utf-8")) for entry in result.stdout.split(b"\0") if entry]


def _previous_manifest_files(deploy_root: Path) -> set[str]:
    manifest_path = _destination_path(deploy_root, Path(DEPLOY_MANIFEST))
    if not manifest_path.is_file():
        return set()
    try:
        contents = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise DeploymentError(f"Invalid processor deployment manifest: {manifest_path}") from error
    files = contents.get("files")
    if not isinstance(files, list) or not all(isinstance(item, str) for item in files):
        raise DeploymentError(f"Invalid processor deployment manifest: {manifest_path}")
    return {_validated_relative_path(item) for item in files}


def _validate_copy_destinations(
    deploy_root: Path,
    tracked_files: list[Path],
    previous_files: set[str],
) -> None:
    for relative_path in tracked_files:
        relative_name = _validated_relative_path(relative_path.as_posix())
        destination = _destination_path(deploy_root, Path(relative_name))
        if (destination.exists() or destination.is_symlink()) and relative_name not in previous_files:
            raise DeploymentError(
                f"Refusing to overwrite unmanaged deployment file: {relative_name}"
            )


def _validated_relative_path(value: str) -> str:
    path = Path(value)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise DeploymentError(f"Invalid managed deployment path: {value}")
    return path.as_posix()


def _destination_path(deploy_root: Path, relative_path: Path) -> Path:
    relative_name = _validated_relative_path(relative_path.as_posix())
    destination = deploy_root / relative_name
    parent = deploy_root
    for part in relative_path.parts[:-1]:
        parent = parent / part
        if parent.is_symlink() or (parent.exists() and not parent.is_dir()):
            raise DeploymentError(
                f"Unsafe deployment destination parent: {parent.relative_to(deploy_root)}"
            )
    if destination.is_symlink():
        raise DeploymentError(f"Deployment destination must not be a symbolic link: {relative_name}")
    if not destination.resolve(strict=False).is_relative_to(deploy_root):
        raise DeploymentError(f"Unsafe deployment destination outside root: {relative_name}")
    return destination


def _remove_managed_file(deploy_root: Path, relative_path: Path) -> None:
    target = _destination_path(deploy_root, relative_path)
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


def _require_expected_service(services: list[str]) -> None:
    if services != [EXPECTED_SERVICE]:
        names = ", ".join(services) or "<none>"
        raise DeploymentError(
            f"Processor deployment must contain only {EXPECTED_SERVICE}; found {names}"
        )


def _verify_revision(
    compose_command: list[str],
    deploy_root: Path,
    environment: dict[str, str],
    commit: str,
) -> None:
    container_id = subprocess.run(
        [*compose_command, "ps", "--quiet", EXPECTED_SERVICE],
        cwd=deploy_root,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if not container_id:
        raise DeploymentError(f"Deployment did not create {EXPECTED_SERVICE}")
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
            f"{EXPECTED_SERVICE} revision {revision or '<missing>'} does not match {commit}"
        )


def main() -> int:
    """Synchronize a checked-out revision and deploy this processor only."""

    parser = argparse.ArgumentParser(description=f"Deploy {EXPECTED_SERVICE}")
    parser.add_argument("--commit", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument(
        "--project-name",
        default=os.environ.get(
            "WAMA_PROCESSOR_COMPOSE_PROJECT_NAME",
            "wama-processor-alarm-threshold",
        ),
    )
    parser.add_argument(
        "--allow-legacy-active-alarm-bootstrap",
        action="store_true",
        help="Allow the reviewed one-time legacy active Alarm recovery bootstrap",
    )
    arguments = parser.parse_args()
    deploy_root_value = os.environ.get("WAMA_PROCESSOR_DEPLOY_ROOT")
    if not deploy_root_value:
        print("Deployment failed: WAMA_PROCESSOR_DEPLOY_ROOT must be configured", file=sys.stderr)
        return 1
    try:
        workspace = Path(os.environ.get("FORGEJO_WORKSPACE", Path.cwd()))
        deploy_root = Path(deploy_root_value)
        synchronize_checkout(workspace, deploy_root, arguments.commit)
        deploy_processor(
            deploy_root,
            arguments.image,
            arguments.commit,
            arguments.project_name,
            os.environ.get("WAMA_INFRA_NETWORK", "wama-infra"),
            allow_legacy_active_alarm_bootstrap=(
                arguments.allow_legacy_active_alarm_bootstrap
            ),
        )
    except (DeploymentError, OSError, subprocess.CalledProcessError) as error:
        print(f"Deployment failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())