"""Marker-owned deployment guard for catalog-derived C37.118 adapters."""

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

import yaml

from gateway_c37_118.catalog import Catalog, load_catalog

DEPLOY_MARKER = ".wama-forgejo-gateway-c37-118-root"
DEPLOY_MANIFEST = ".wama-forgejo-gateway-c37-118-manifest.json"
ADAPTER_MANIFEST = ".wama-forgejo-gateway-c37-118-adapters.json"
OVERLAY = "generated-adapters.compose.yaml"
SERVICE_PREFIX = "c37-118-gateway-"
_SAFE_SERVICE = re.compile(r"^c37-118-gateway-[a-z][a-z0-9-]{1,62}$")


class DeploymentError(RuntimeError):
    """Raised when a requested deployment could cross the gateway boundary."""


def synchronize_checkout(workspace: Path, deploy_root: Path, commit: str, image: str = "") -> None:
    """Copy only tracked gateway files into the dedicated marked root."""

    workspace = workspace.resolve()
    deploy_root = _deploy_root(workspace, deploy_root)
    if not (workspace / "compose.yaml").is_file() or (workspace / "Dockerfile").exists() is False:
        raise DeploymentError("Forgejo checkout is not a C37.118 gateway repository")
    if (workspace / "docker-compose.yml").exists():
        raise DeploymentError("Gateway deployment must not use an infrastructure checkout")
    output = subprocess.run(
        ["git", "-C", str(workspace), "ls-files", "-z"], check=True, capture_output=True
    ).stdout
    tracked = [Path(item.decode("utf-8")) for item in output.split(b"\0") if item]
    previous = _managed_files(deploy_root / DEPLOY_MANIFEST)
    current = {path.as_posix() for path in tracked}
    for name in previous.difference(current):
        target = _destination(deploy_root, Path(name))
        if target.is_file() or target.is_symlink():
            target.unlink()
    for relative in tracked:
        destination = _destination(deploy_root, relative)
        name = relative.as_posix()
        if (destination.exists() or destination.is_symlink()) and name not in previous:
            raise DeploymentError(f"Refusing to overwrite unmanaged deployment file: {name}")
        if (workspace / relative).is_symlink():
            raise DeploymentError(f"Tracked symbolic links are not supported: {name}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(workspace / relative, destination)
    (deploy_root / DEPLOY_MANIFEST).write_text(
        json.dumps({"commit": commit, "image": image, "files": sorted(current)}, indent=2) + "\n",
        encoding="utf-8",
    )


def render_overlay(catalog: Catalog, image: str) -> dict[str, Any]:
    """Render only approved source adapter services on the external network."""

    services: dict[str, Any] = {}
    for source in catalog.sources:
        name = _service_name(source.source_id)
        services[name] = {
            "image": image,
            "command": ["python", "-m", "gateway_c37_118.gateway_runtime"],
            "environment": {
                "WAMA_CATALOG_DIR": "/app/catalog",
                "WAMA_SOURCE_ID": source.source_id,
                "WAMA_KAFKA_BOOTSTRAP_SERVERS": "${WAMA_KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}",
                "WAMA_LIVE_MEASUREMENT_TOPIC": "${WAMA_LIVE_MEASUREMENT_TOPIC:-LiveMeasurement}",
            },
            "labels": {
                "io.wama.gateway-c37-118.managed": "true",
                "io.wama.gateway-c37-118.source-id": source.source_id,
            },
            "networks": ["wama-infra"],
            "restart": "unless-stopped",
        }
    return {
        "services": services,
        "networks": {"wama-infra": {"external": True, "name": "${WAMA_INFRA_NETWORK:-wama-infra}"}},
    }


def deploy_adapters(deploy_root: Path, image: str, commit: str, project: str, network: str) -> None:
    """Reconcile only generated adapters after one-shot Masterdata succeeds."""

    _validate_base_compose(deploy_root / "compose.yaml")
    catalog = load_catalog(deploy_root / "catalog")
    subprocess.run(["docker", "network", "inspect", network], check=True)
    overlay = render_overlay(catalog, image)
    (deploy_root / OVERLAY).write_text(yaml.safe_dump(overlay, sort_keys=False), encoding="utf-8")
    environment = os.environ.copy()
    environment["WAMA_INFRA_NETWORK"] = network
    environment["WAMA_GATEWAY_IMAGE"] = image
    environment["WAMA_CATALOG_REVISION"] = commit
    command = ["docker", "compose", "--project-name", project, "-f", "compose.yaml", "-f", OVERLAY]
    services = _compose_services(command, deploy_root, environment)
    expected = [_service_name(source.source_id) for source in catalog.sources] + ["masterdata-publisher"]
    if sorted(services) != sorted(expected):
        raise DeploymentError("Generated gateway Compose contains an unexpected service")
    _run(command, deploy_root, environment, "pull", "masterdata-publisher")
    _verify_image_revision(image, commit)
    _run(command, deploy_root, environment, "run", "--rm", "--no-deps", "masterdata-publisher")
    previous = _managed_adapters(deploy_root / ADAPTER_MANIFEST)
    current = {_service_name(source.source_id) for source in catalog.sources}
    for stale in sorted(previous.difference(current)):
        _remove_adapter(project, stale)
    if current:
        _run(command, deploy_root, environment, "pull", *sorted(current))
        _run(command, deploy_root, environment, "up", "-d", *sorted(current))
        for service in sorted(current):
            _verify_revision(command, deploy_root, environment, service, commit)
    (deploy_root / ADAPTER_MANIFEST).write_text(
        json.dumps({"services": sorted(current)}, indent=2) + "\n", encoding="utf-8"
    )
    if current:
        _run(
            command, deploy_root, environment, "run", "--rm", "--no-deps",
            "masterdata-publisher", "python", "-m", "gateway_c37_118.verify_live_measurements",
        )


def _deploy_root(workspace: Path, deploy_root: Path) -> Path:
    if not deploy_root.is_absolute() or deploy_root.is_symlink():
        raise DeploymentError("WAMA_GATEWAY_C37_118_DEPLOY_ROOT must be an absolute non-symbolic path")
    deploy_root = deploy_root.resolve()
    if deploy_root in {Path("/"), workspace} or deploy_root.is_relative_to(workspace):
        raise DeploymentError("Gateway deploy root is unsafe")
    marker = deploy_root / DEPLOY_MARKER
    if marker.is_symlink() or not marker.is_file():
        raise DeploymentError(f"Gateway deploy root lacks {DEPLOY_MARKER}")
    return deploy_root


def _destination(root: Path, relative: Path) -> Path:
    if relative.is_absolute() or not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
        raise DeploymentError(f"Invalid managed deployment path: {relative}")
    parent = root
    for part in relative.parts[:-1]:
        parent /= part
        if parent.is_symlink() or (parent.exists() and not parent.is_dir()):
            raise DeploymentError(f"Unsafe deployment destination parent: {relative}")
    return root / relative


def _managed_files(path: Path) -> set[str]:
    if not path.is_file():
        return set()
    try:
        files = json.loads(path.read_text(encoding="utf-8"))["files"]
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as error:
        raise DeploymentError("Invalid gateway deployment manifest") from error
    if not isinstance(files, list) or not all(isinstance(name, str) for name in files):
        raise DeploymentError("Invalid gateway deployment manifest")
    return set(files)


def _managed_adapters(path: Path) -> set[str]:
    if not path.is_file():
        return set()
    try:
        services = json.loads(path.read_text(encoding="utf-8"))["services"]
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as error:
        raise DeploymentError("Invalid gateway adapter manifest") from error
    if not isinstance(services, list) or not all(isinstance(name, str) and _SAFE_SERVICE.fullmatch(name) for name in services):
        raise DeploymentError("Invalid gateway adapter manifest")
    return set(services)


def _service_name(source_id: str) -> str:
    service = f"{SERVICE_PREFIX}{source_id}"
    if not _SAFE_SERVICE.fullmatch(service):
        raise DeploymentError(f"Unsafe catalog source service name: {source_id}")
    return service


def _validate_base_compose(path: Path) -> None:
    """Reject an edited application Compose file before Docker receives it."""

    try:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as error:
        raise DeploymentError("Gateway Compose file is invalid") from error
    if not isinstance(document, dict) or set(document.get("services", {})) != {"masterdata-publisher"}:
        raise DeploymentError("Gateway Compose must contain only masterdata-publisher before generation")
    service = document["services"]["masterdata-publisher"]
    if not isinstance(service, dict):
        raise DeploymentError("masterdata-publisher must be a Compose service mapping")
    if service.get("networks") != ["wama-infra"] or "network_mode" in service:
        raise DeploymentError("masterdata-publisher must use only the external wama-infra network")
    if "ports" in service or "expose" in service:
        raise DeploymentError("masterdata-publisher must not expose network ports")
    networks = document.get("networks")
    network = networks.get("wama-infra") if isinstance(networks, dict) else None
    if not isinstance(network, dict) or network.get("external") is not True or set(networks) != {"wama-infra"}:
        raise DeploymentError("Gateway Compose must use only the external wama-infra network")


def _run(command: list[str], root: Path, environment: dict[str, str], *arguments: str) -> None:
    subprocess.run([*command, *arguments], cwd=root, env=environment, check=True)


def _compose_services(command: list[str], root: Path, environment: dict[str, str]) -> list[str]:
    result = subprocess.run(
        [*command, "config", "--services"], cwd=root, env=environment,
        check=True, capture_output=True, text=True,
    )
    return [line for line in result.stdout.splitlines() if line]


def _remove_adapter(project: str, service: str) -> None:
    """Remove only a previously recorded adapter with all ownership labels."""

    if not _SAFE_SERVICE.fullmatch(service):
        raise DeploymentError("Refusing to remove a non-gateway adapter")
    source_id = service.removeprefix(SERVICE_PREFIX)
    result = subprocess.run(
        [
            "docker", "ps", "--all", "--quiet",
            "--filter", f"label=com.docker.compose.project={project}",
            "--filter", f"label=com.docker.compose.service={service}",
        ],
        check=True, capture_output=True, text=True,
    )
    identifiers = [item for item in result.stdout.splitlines() if item]
    for identifier in identifiers:
        labels = subprocess.run(
            ["docker", "inspect", "--format", "{{json .Config.Labels}}", identifier],
            check=True, capture_output=True, text=True,
        ).stdout
        try:
            parsed = json.loads(labels)
        except json.JSONDecodeError as error:
            raise DeploymentError(f"Unable to inspect managed adapter {service}") from error
        if (
            parsed.get("io.wama.gateway-c37-118.managed") != "true"
            or parsed.get("io.wama.gateway-c37-118.source-id") != source_id
        ):
            raise DeploymentError(f"Refusing to remove adapter without matching ownership labels: {service}")
        subprocess.run(["docker", "rm", "--force", identifier], check=True)


def _verify_revision(command: list[str], root: Path, environment: dict[str, str], service: str, commit: str) -> None:
    identifier = subprocess.run(
        [*command, "ps", "--quiet", service], cwd=root, env=environment,
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    if not identifier:
        raise DeploymentError(f"Deployment did not create {service}")
    revision = subprocess.run(
        ["docker", "inspect", "--format", '{{ index .Config.Labels "org.opencontainers.image.revision" }}', identifier],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    if revision != commit:
        raise DeploymentError(f"{service} revision does not match {commit}")


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
        raise DeploymentError(f"{image} revision does not match {commit}")


def main() -> int:
    """Deploy this checkout only through its dedicated marker-owned root."""

    parser = argparse.ArgumentParser(description="Deploy catalog-derived C37.118 adapters")
    parser.add_argument("--commit", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--project-name", default="wama-gateway-c37-118")
    arguments = parser.parse_args()
    root_value = os.environ.get("WAMA_GATEWAY_C37_118_DEPLOY_ROOT")
    if not root_value:
        print("Deployment failed: WAMA_GATEWAY_C37_118_DEPLOY_ROOT must be configured", file=sys.stderr)
        return 1
    try:
        root = Path(root_value)
        synchronize_checkout(
            Path(os.environ.get("FORGEJO_WORKSPACE", Path.cwd())),
            root,
            arguments.commit,
            arguments.image,
        )
        deploy_adapters(root, arguments.image, arguments.commit, arguments.project_name, os.environ.get("WAMA_INFRA_NETWORK", "wama-infra"))
    except (DeploymentError, OSError, subprocess.CalledProcessError) as error:
        print(f"Deployment failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
