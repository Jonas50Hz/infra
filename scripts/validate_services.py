"""Validate the Compose service contract before running the Forgejo pipeline."""

from __future__ import annotations

from pathlib import Path
import re
import subprocess
import sys

PROCESSOR_NAME_PATTERN = re.compile(r"processor-[a-z0-9]+(?:-[a-z0-9]+)*\Z")
INCLUDE_PATTERN = re.compile(r"^\s*-\s+\./services/([^/]+)/compose\.yaml\s*$", re.MULTILINE)


def validate_structure(repository_root: Path) -> list[str]:
    """Return contract failures without requiring Docker to be installed."""

    errors: list[str] = []
    compose_path = repository_root / "docker-compose.yml"
    services_directory = repository_root / "services"
    compose_contents = compose_path.read_text(encoding="utf-8")
    included_services = set(INCLUDE_PATTERN.findall(compose_contents))

    if "processor-template" in included_services:
        errors.append("processor-template must not be included in docker-compose.yml")

    errors.extend(_validate_pmu_gateway(services_directory, included_services))

    for service_name in sorted(included_services):
        fragment = services_directory / service_name / "compose.yaml"
        if not fragment.is_file():
            errors.append(f"Compose include has no matching fragment: services/{service_name}/compose.yaml")

    processor_directories = sorted(
        path
        for path in services_directory.iterdir()
        if path.is_dir() and path.name.startswith("processor-")
    )
    for processor_directory in processor_directories:
        errors.extend(_validate_processor(processor_directory, included_services))

    return errors


def _validate_pmu_gateway(services_directory: Path, included_services: set[str]) -> list[str]:
    service_name = "pmu-gateway"
    compose_path = services_directory / service_name / "compose.yaml"
    if not compose_path.is_file():
        return ["pmu-gateway is missing compose.yaml"]
    if service_name not in included_services:
        return ["pmu-gateway is missing from docker-compose.yml includes"]

    compose_contents = compose_path.read_text(encoding="utf-8")
    if "/pmu-gateway:${WAMA_IMAGE_TAG:-main}" not in compose_contents:
        return ["pmu-gateway image must use the WAMA_IMAGE_TAG main deployment tag"]
    return []


def _validate_processor(processor_directory: Path, included_services: set[str]) -> list[str]:
    service_name = processor_directory.name
    errors: list[str] = []
    if not PROCESSOR_NAME_PATTERN.fullmatch(service_name):
        return [f"Invalid processor service name: {service_name}"]
    if service_name not in included_services:
        errors.append(f"{service_name} is missing from docker-compose.yml includes")

    required_paths = (
        "compose.yaml",
        "Dockerfile",
        "README.md",
        "processor.yaml",
        "requirements.txt",
        "src",
        "tests",
    )
    for required_path in required_paths:
        if not (processor_directory / required_path).exists():
            errors.append(f"{service_name} is missing {required_path}")

    compose_path = processor_directory / "compose.yaml"
    dockerfile_path = processor_directory / "Dockerfile"
    config_path = processor_directory / "processor.yaml"
    if not compose_path.is_file() or not dockerfile_path.is_file() or not config_path.is_file():
        return errors

    compose_contents = compose_path.read_text(encoding="utf-8")
    dockerfile_contents = dockerfile_path.read_text(encoding="utf-8")
    config_contents = config_path.read_text(encoding="utf-8")
    package_name = service_name.replace("-", "_")

    if not re.search(rf"^\s{{2}}{re.escape(service_name)}:\s*$", compose_contents, re.MULTILINE):
        errors.append(f"{service_name} compose fragment does not declare its matching service")
    if "kafka-init:" not in compose_contents:
        errors.append(f"{service_name} must wait for kafka-init")
    if f"/{service_name}:${{WAMA_IMAGE_TAG:-main}}" not in compose_contents:
        errors.append(f"{service_name} image must use the WAMA_IMAGE_TAG main deployment tag")
    if "processor-template" in compose_contents or "templates/quixstreams-processor" in compose_contents:
        errors.append(f"{service_name} compose fragment still references the template")
    if f"services/{service_name}/" not in dockerfile_contents:
        errors.append(f"{service_name} Dockerfile does not own its source paths")
    if "processor_template" in dockerfile_contents:
        errors.append(f"{service_name} Dockerfile still references the template package")
    if f"consumer_group: {service_name}" not in config_contents:
        errors.append(f"{service_name} must use a distinct matching consumer group")
    if not (processor_directory / "src" / package_name).is_dir():
        errors.append(f"{service_name} is missing src/{package_name}")

    return errors


def validate_compose(repository_root: Path) -> int:
    """Run Compose validation only after structural checks have passed."""

    result = subprocess.run(
        ["docker", "compose", "config", "--quiet"],
        cwd=repository_root,
        check=False,
    )
    return result.returncode


def main() -> int:
    """Print service contract failures and validate the assembled Compose file."""

    repository_root = Path(__file__).resolve().parents[1]
    errors = validate_structure(repository_root)
    if errors:
        for error in errors:
            print(f"Service validation failed: {error}", file=sys.stderr)
        return 1
    return validate_compose(repository_root)


if __name__ == "__main__":
    raise SystemExit(main())