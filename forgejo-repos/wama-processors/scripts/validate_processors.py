"""Validate the processors repository without reading infrastructure files."""

from __future__ import annotations

from pathlib import Path
import re
import subprocess
import sys

PROCESSOR_PATTERN = re.compile(r"processor-[a-z0-9]+(?:-[a-z0-9]+)*\Z")
INCLUDE_PATTERN = re.compile(r"^\s*-\s+\./processors/([^/]+)/compose\.yaml\s*$", re.MULTILINE)
REQUIRED_PROCESSOR = "processor-frequency-scale"


def validate_structure(application_root: Path) -> list[str]:
    """Return processors-repository layout failures without a Docker daemon."""

    errors: list[str] = []
    compose_path = application_root / "compose.yaml"
    if not compose_path.is_file():
        return ["Processors compose.yaml is missing"]

    compose_contents = compose_path.read_text(encoding="utf-8")
    if "external: true" not in compose_contents or "wama-infra" not in compose_contents:
        errors.append("Processors compose.yaml must declare the external wama-infra network")
    if not (application_root / "processors" / REQUIRED_PROCESSOR).is_dir():
        errors.append(f"Required processor is missing: {REQUIRED_PROCESSOR}")
    included_processors = set(INCLUDE_PATTERN.findall(compose_contents))
    for processor_directory in sorted((application_root / "processors").iterdir()):
        if not processor_directory.is_dir() or not processor_directory.name.startswith("processor-"):
            continue
        errors.extend(_validate_processor(processor_directory, included_processors))

    return errors


def _validate_processor(processor_directory: Path, included_processors: set[str]) -> list[str]:
    service_name = processor_directory.name
    errors: list[str] = []
    if not PROCESSOR_PATTERN.fullmatch(service_name):
        return [f"Invalid processor name: {service_name}"]
    if service_name not in included_processors:
        errors.append(f"{service_name} is missing from processors compose includes")

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
    if "depends_on:" in compose_contents:
        errors.append(f"{service_name} must not use cross-project depends_on")
    if "wama-infra" not in compose_contents or "external: true" not in compose_contents:
        errors.append(f"{service_name} must attach to the external wama-infra network")
    if f"/{service_name}:${{WAMA_PROCESSORS_IMAGE_TAG:-main}}" not in compose_contents:
        errors.append(f"{service_name} must use the processors main image tag")
    if f"processors/{service_name}/" not in dockerfile_contents:
        errors.append(f"{service_name} Dockerfile must own only processors source paths")
    if "templates/quixstreams-processor" in dockerfile_contents or "processor_template" in dockerfile_contents:
        errors.append(f"{service_name} Dockerfile still references the template")
    if "COPY contracts/rtd_schema.proto" not in dockerfile_contents:
        errors.append(f"{service_name} Dockerfile must use the processors contract copy")
    if f"consumer_group: {service_name}" not in config_contents:
        errors.append(f"{service_name} must use a distinct matching consumer group")
    if not (processor_directory / "src" / package_name).is_dir():
        errors.append(f"{service_name} is missing src/{package_name}")
    return errors


def validate_compose(application_root: Path) -> int:
    """Render only processors Compose files after structural validation."""

    return subprocess.run(
        ["docker", "compose", "-f", "compose.yaml", "config", "--quiet"],
        cwd=application_root,
        check=False,
    ).returncode


def main() -> int:
    """Print processors validation failures and render its Compose assembly."""

    application_root = Path(__file__).resolve().parents[1]
    errors = validate_structure(application_root)
    if errors:
        for error in errors:
            print(f"Processors validation failed: {error}", file=sys.stderr)
        return 1
    return validate_compose(application_root)


if __name__ == "__main__":
    raise SystemExit(main())