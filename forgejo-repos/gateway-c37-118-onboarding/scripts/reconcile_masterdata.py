"""Synchronize and run only the one-shot Masterdata publisher service."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
from typing import Any


PUBLISHER_SERVICE = "masterdata-publisher"
GATEWAY_SERVICE_PREFIX = "c37-118-gateway-"
GATEWAY_COMPOSE_FILE = ".wama-forgejo-gateway-onboarding-gateways.json"
GATEWAY_STATE_FILE = ".wama-forgejo-gateway-onboarding-gateways.json.state"
DEPLOY_MARKER = ".wama-forgejo-gateway-onboarding-root"
DEPLOY_MANIFEST = ".wama-forgejo-gateway-onboarding-manifest.json"
SOURCE_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")


class DeploymentError(RuntimeError):
    """Raised when a gateway-onboarding deployment target is unsafe."""


def synchronize_checkout(workspace: Path, deploy_root: Path, commit: str) -> None:
    """Copy tracked files into only the marked, isolated deployment root."""

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
    (deploy_root / DEPLOY_MANIFEST).write_text(
        json.dumps({"commit": commit, "files": sorted(current_files)}, indent=2) + "\n",
        encoding="utf-8",
    )


def reconcile_masterdata(
    deploy_root: Path,
    image: str,
    commit: str,
    project_name: str,
    infra_network: str,
) -> None:
    """Reconcile Masterdata and only its approved per-source gateway adapters."""

    subprocess.run(["docker", "network", "inspect", infra_network], check=True)
    environment = os.environ.copy()
    environment["WAMA_MASTERDATA_PUBLISHER_IMAGE"] = image
    environment["WAMA_C37_118_GATEWAY_IMAGE"] = image
    environment["WAMA_GATEWAY_ONBOARDING_COMPOSE_PROJECT_NAME"] = project_name
    environment["WAMA_INFRA_NETWORK"] = infra_network
    environment["WAMA_MASTERDATA_CATALOG_REVISION"] = commit
    base_compose_command = [
        "docker",
        "compose",
        "--project-name",
        project_name,
        "-f",
        "compose.yaml",
    ]
    _run_compose(base_compose_command, deploy_root, environment, "config", "--quiet")
    _require_expected_service(_compose_services(base_compose_command, deploy_root, environment))
    _require_external_infra_network(
        _rendered_compose_config(base_compose_command, deploy_root, environment),
        infra_network,
    )
    _run_compose(base_compose_command, deploy_root, environment, "pull", PUBLISHER_SERVICE)
    _verify_image_revision(image, commit)
    _run_compose(
        base_compose_command,
        deploy_root,
        environment,
        "run",
        "--rm",
        "--no-deps",
        PUBLISHER_SERVICE,
    )

    previous_gateway_services = _previous_gateway_services(deploy_root)
    source_ids = _active_source_ids(deploy_root)
    gateway_services = tuple(_gateway_service_name(source_id) for source_id in source_ids)
    _write_gateway_compose(deploy_root, source_ids)
    gateway_compose_command = [*base_compose_command, "-f", GATEWAY_COMPOSE_FILE]
    _run_compose(gateway_compose_command, deploy_root, environment, "config", "--quiet")
    _require_expected_services(
        _compose_services(gateway_compose_command, deploy_root, environment),
        (PUBLISHER_SERVICE, *gateway_services),
    )
    rendered = _rendered_compose_config(gateway_compose_command, deploy_root, environment)
    _require_external_infra_network(rendered, infra_network)
    _require_only_infra_network(rendered, (PUBLISHER_SERVICE, *gateway_services))

    stale_gateway_services = tuple(
        service for service in previous_gateway_services if service not in gateway_services
    )
    _remove_stale_gateway_containers(project_name, stale_gateway_services)
    if gateway_services:
        _run_compose(
            gateway_compose_command,
            deploy_root,
            environment,
            "up",
            "-d",
            "--no-deps",
            *gateway_services,
        )
        _verify_gateway_revisions(
            gateway_compose_command,
            deploy_root,
            environment,
            gateway_services,
            commit,
        )
    _write_gateway_state(deploy_root, commit, gateway_services)


def _validate_workspace(workspace: Path) -> None:
    if not (workspace / "compose.yaml").is_file() or not (workspace / "Dockerfile").is_file():
        raise DeploymentError("Forgejo checkout is not a gateway-onboarding repository")
    if not (workspace / "catalog" / "sources").is_dir():
        raise DeploymentError("Gateway-onboarding checkout has no source catalog")
    if (workspace / "docker-compose.yml").exists():
        raise DeploymentError("Gateway onboarding must not use an infrastructure checkout")


def _validate_deploy_root(workspace: Path, deploy_root: Path) -> Path:
    if not deploy_root.is_absolute():
        raise DeploymentError("WAMA_GATEWAY_ONBOARDING_DEPLOY_ROOT must be an absolute path")
    if deploy_root.is_symlink():
        raise DeploymentError("WAMA_GATEWAY_ONBOARDING_DEPLOY_ROOT must not be a symbolic link")
    deploy_root = deploy_root.resolve()
    if deploy_root == Path("/"):
        raise DeploymentError("WAMA_GATEWAY_ONBOARDING_DEPLOY_ROOT must not be /")
    if deploy_root == workspace or deploy_root.is_relative_to(workspace):
        raise DeploymentError(
            "WAMA_GATEWAY_ONBOARDING_DEPLOY_ROOT must not be inside the Forgejo workspace"
        )
    marker = deploy_root / DEPLOY_MARKER
    if marker.is_symlink() or not marker.is_file():
        raise DeploymentError(f"Gateway-onboarding deploy root lacks {DEPLOY_MARKER}")
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
        raise DeploymentError(f"Invalid deployment manifest: {manifest_path}") from error
    files = contents.get("files")
    if not isinstance(files, list) or not all(isinstance(item, str) for item in files):
        raise DeploymentError(f"Invalid deployment manifest: {manifest_path}")
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
    return destination


def _remove_managed_file(deploy_root: Path, relative_path: Path) -> None:
    target = _destination_path(deploy_root, relative_path)
    if target.is_file() or target.is_symlink():
        target.unlink()


def _active_source_ids(deploy_root: Path) -> tuple[str, ...]:
    source_directory = deploy_root / "catalog" / "sources"
    if source_directory.is_symlink() or not source_directory.is_dir():
        raise DeploymentError("Gateway-onboarding deployment has no safe source catalog")
    source_ids: list[str] = []
    for source_path in sorted(source_directory.glob("*.yaml")):
        if source_path.is_symlink() or not source_path.is_file():
            raise DeploymentError(f"Gateway-onboarding source is not a regular file: {source_path.name}")
        source_id = source_path.stem
        if not SOURCE_ID_PATTERN.fullmatch(source_id):
            raise DeploymentError(f"Gateway-onboarding source ID is unsafe: {source_id!r}")
        source_ids.append(source_id)
    return tuple(source_ids)


def _gateway_service_name(source_id: str) -> str:
    if not SOURCE_ID_PATTERN.fullmatch(source_id):
        raise DeploymentError(f"Gateway-onboarding source ID is unsafe: {source_id!r}")
    return f"{GATEWAY_SERVICE_PREFIX}{source_id}"


def _previous_gateway_services(deploy_root: Path) -> tuple[str, ...]:
    state_path = deploy_root / GATEWAY_STATE_FILE
    if not state_path.exists():
        return ()
    if state_path.is_symlink() or not state_path.is_file():
        raise DeploymentError("Gateway-onboarding gateway state is unsafe")
    try:
        state = json.loads(state_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise DeploymentError("Gateway-onboarding gateway state is invalid") from error
    services = state.get("services") if isinstance(state, dict) else None
    if not isinstance(services, list) or not all(isinstance(service, str) for service in services):
        raise DeploymentError("Gateway-onboarding gateway state is invalid")
    if len(services) != len(set(services)):
        raise DeploymentError("Gateway-onboarding gateway state repeats a service")
    for service in services:
        _validate_gateway_service_name(service)
    return tuple(services)


def _write_gateway_compose(deploy_root: Path, source_ids: tuple[str, ...]) -> None:
    compose_path = deploy_root / GATEWAY_COMPOSE_FILE
    state_path = deploy_root / GATEWAY_STATE_FILE
    if compose_path.is_symlink():
        raise DeploymentError("Gateway-onboarding generated Compose file is unsafe")
    if compose_path.exists() and not state_path.exists():
        raise DeploymentError("Refusing to overwrite unmanaged generated gateway Compose file")

    services: dict[str, dict[str, object]] = {}
    for source_id in source_ids:
        service_name = _gateway_service_name(source_id)
        services[service_name] = {
            "image": "${WAMA_C37_118_GATEWAY_IMAGE}",
            "command": ["python", "-m", "gateway_c37_118_onboarding.gateway_main"],
            "environment": {
                "KAFKA_BOOTSTRAP_SERVERS": "${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}",
                "WAMA_MASTERDATA_CATALOG_DIRECTORY": "/app/catalog/sources",
                "WAMA_MASTERDATA_CATALOG_ID": "${WAMA_MASTERDATA_CATALOG_ID:-wama-c37-118-onboarding}",
                "WAMA_MASTERDATA_CATALOG_REVISION": "${WAMA_MASTERDATA_CATALOG_REVISION:-development}",
                "WAMA_C37_118_GATEWAY_SOURCE_ID": source_id,
                "WAMA_LIVE_MEASUREMENT_TOPIC": "${WAMA_LIVE_MEASUREMENT_TOPIC:-LiveMeasurement}",
            },
            "networks": ["wama-infra"],
            "restart": "unless-stopped",
        }
    compose_path.write_text(json.dumps({"services": services}, indent=2) + "\n", encoding="utf-8")


def _write_gateway_state(deploy_root: Path, commit: str, services: tuple[str, ...]) -> None:
    state_path = deploy_root / GATEWAY_STATE_FILE
    if state_path.is_symlink():
        raise DeploymentError("Gateway-onboarding gateway state is unsafe")
    state_path.write_text(
        json.dumps({"commit": commit, "services": list(services)}, indent=2) + "\n",
        encoding="utf-8",
    )


def _remove_stale_gateway_containers(project_name: str, services: tuple[str, ...]) -> None:
    for service in services:
        _validate_gateway_service_name(service)
        result = subprocess.run(
            [
                "docker",
                "ps",
                "--quiet",
                "--filter",
                f"label=com.docker.compose.project={project_name}",
                "--filter",
                f"label=com.docker.compose.service={service}",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        container_ids = [container_id for container_id in result.stdout.splitlines() if container_id]
        if container_ids:
            subprocess.run(["docker", "rm", "--force", *container_ids], check=True)


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


def _rendered_compose_config(
    compose_command: list[str],
    deploy_root: Path,
    environment: dict[str, str],
) -> Any:
    result = subprocess.run(
        [*compose_command, "config", "--format", "json"],
        cwd=deploy_root,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise DeploymentError("Rendered gateway-onboarding Compose config is not JSON") from error


def _require_expected_service(services: list[str]) -> None:
    _require_expected_services(services, (PUBLISHER_SERVICE,))


def _require_expected_services(services: list[str], expected_services: tuple[str, ...]) -> None:
    if len(services) != len(expected_services) or set(services) != set(expected_services):
        names = ", ".join(services) or "<none>"
        expected_names = ", ".join(expected_services) or "<none>"
        raise DeploymentError(
            f"Gateway onboarding must contain only {expected_names}; found {names}"
        )


def _require_external_infra_network(rendered: Any, infra_network: str) -> None:
    if not isinstance(rendered, dict):
        raise DeploymentError("Rendered gateway-onboarding Compose config must be an object")
    networks = rendered.get("networks")
    if not isinstance(networks, dict):
        raise DeploymentError("Gateway onboarding must declare the external wama-infra network")
    network = networks.get("wama-infra")
    if not isinstance(network, dict):
        raise DeploymentError("Gateway onboarding must declare the wama-infra network")
    if network.get("external") is not True or network.get("name") != infra_network:
        raise DeploymentError("Gateway onboarding must use only the existing external wama-infra network")


def _require_only_infra_network(rendered: Any, services: tuple[str, ...]) -> None:
    if not isinstance(rendered, dict) or not isinstance(rendered.get("services"), dict):
        raise DeploymentError("Rendered gateway-onboarding Compose config has no services")
    rendered_services = rendered["services"]
    for service_name in services:
        service = rendered_services.get(service_name)
        if not isinstance(service, dict):
            raise DeploymentError(f"Rendered gateway-onboarding service is missing: {service_name}")
        networks = service.get("networks")
        if isinstance(networks, list):
            network_names = set(networks)
        elif isinstance(networks, dict):
            network_names = set(networks)
        else:
            raise DeploymentError(f"Gateway-onboarding service has no network: {service_name}")
        if network_names != {"wama-infra"}:
            raise DeploymentError(
                f"Gateway-onboarding service must use only wama-infra: {service_name}"
            )


def _verify_gateway_revisions(
    compose_command: list[str],
    deploy_root: Path,
    environment: dict[str, str],
    services: tuple[str, ...],
    commit: str,
) -> None:
    for service in services:
        _validate_gateway_service_name(service)
        result = subprocess.run(
            [*compose_command, "ps", "--quiet", service],
            cwd=deploy_root,
            env=environment,
            check=True,
            capture_output=True,
            text=True,
        )
        container_ids = [container_id for container_id in result.stdout.splitlines() if container_id]
        if len(container_ids) != 1:
            raise DeploymentError(f"Gateway deployment did not create exactly one {service} container")
        revision = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                '{{ index .Config.Labels "org.opencontainers.image.revision" }}',
                container_ids[0],
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if revision != commit:
            raise DeploymentError(
                f"{service} revision {revision or '<missing>'} does not match {commit}"
            )


def _validate_gateway_service_name(service: str) -> None:
    if not service.startswith(GATEWAY_SERVICE_PREFIX):
        raise DeploymentError(f"Gateway-onboarding gateway service is unsafe: {service!r}")
    source_id = service.removeprefix(GATEWAY_SERVICE_PREFIX)
    if not SOURCE_ID_PATTERN.fullmatch(source_id):
        raise DeploymentError(f"Gateway-onboarding gateway service is unsafe: {service!r}")


def _verify_image_revision(image: str, commit: str) -> None:
    revision = subprocess.run(
        [
            "docker",
            "image",
            "inspect",
            "--format",
            '{{ index .Config.Labels "org.opencontainers.image.revision" }}',
            image,
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if revision != commit:
        raise DeploymentError(
            f"Gateway-onboarding image revision {revision or '<missing>'} does not match {commit}"
        )


def main() -> int:
    """Synchronize a reviewed catalog revision and run it once against Kafka."""

    parser = argparse.ArgumentParser(description="Reconcile C37.118 Masterdata")
    parser.add_argument("--commit", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument(
        "--project-name",
        default=os.environ.get(
            "WAMA_GATEWAY_ONBOARDING_COMPOSE_PROJECT_NAME",
            "wama-gateway-c37-118-onboarding",
        ),
    )
    arguments = parser.parse_args()
    deploy_root_value = os.environ.get("WAMA_GATEWAY_ONBOARDING_DEPLOY_ROOT")
    if not deploy_root_value:
        print(
            "Deployment failed: WAMA_GATEWAY_ONBOARDING_DEPLOY_ROOT must be configured",
            file=sys.stderr,
        )
        return 1
    try:
        workspace = Path(os.environ.get("FORGEJO_WORKSPACE", Path.cwd()))
        deploy_root = Path(deploy_root_value)
        synchronize_checkout(workspace, deploy_root, arguments.commit)
        reconcile_masterdata(
            deploy_root,
            arguments.image,
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