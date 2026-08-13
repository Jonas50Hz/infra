"""Create a tracked processor service from the editable Quixstreams template."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import shutil
import sys

PROCESSOR_SLUG_PATTERN = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*\Z")
PROCESSOR_TABLE_START = "<!-- provisioned-processor-services:start -->"
PROCESSOR_TABLE_END = "<!-- provisioned-processor-services:end -->"
PROCESSOR_INSTRUCTIONS_START = "<!-- provisioned-processors:start -->"
PROCESSOR_INSTRUCTIONS_END = "<!-- provisioned-processors:end -->"


class ProvisioningError(ValueError):
    """Raised when a processor service cannot be created safely."""


def provision_processor(repository_root: Path, slug: str) -> str:
    """Copy the template, specialize identifiers, and register one processor service."""

    _validate_slug(slug)
    service_name = f"processor-{slug}"
    package_name = service_name.replace("-", "_")
    config_source_variable = f"{package_name.upper()}_CONFIG_SOURCE"
    template_directory = repository_root / "templates" / "quixstreams-processor"
    service_directory = repository_root / "services" / service_name

    if not template_directory.is_dir():
        raise ProvisioningError(f"Processor template is missing: {template_directory}")
    if service_directory.exists():
        raise ProvisioningError(f"Processor service already exists: {service_directory}")

    shutil.copytree(template_directory, service_directory)
    try:
        _replace_template_identifiers(
            service_directory,
            service_name,
            package_name,
            config_source_variable,
        )
        template_package_directory = service_directory / "src" / "processor_template"
        template_package_directory.rename(service_directory / "src" / package_name)
        _add_compose_include(repository_root / "docker-compose.yml", service_name)
        _update_processor_documentation(repository_root)
    except Exception:
        shutil.rmtree(service_directory)
        raise

    return service_name


def _validate_slug(slug: str) -> None:
    if not PROCESSOR_SLUG_PATTERN.fullmatch(slug):
        raise ProvisioningError(
            "Processor name must use lowercase letters, numbers, and single hyphens"
        )


def _replace_template_identifiers(
    service_directory: Path,
    service_name: str,
    package_name: str,
    config_source_variable: str,
) -> None:
    replacements = (
        ("templates/quixstreams-processor", f"services/{service_name}"),
        ("processor-template", service_name),
        ("processor_template", package_name),
        ("PROCESSOR_TEMPLATE_CONFIG_SOURCE", config_source_variable),
    )
    for path in service_directory.rglob("*"):
        if not path.is_file():
            continue
        contents = path.read_text(encoding="utf-8")
        for source, target in replacements:
            contents = contents.replace(source, target)
        path.write_text(contents, encoding="utf-8")


def _add_compose_include(compose_path: Path, service_name: str) -> None:
    include = f"  - ./services/{service_name}/compose.yaml\n"
    contents = compose_path.read_text(encoding="utf-8")
    if include in contents:
        return

    anchor = "  - ./services/pmu-gateway/compose.yaml\n"
    if anchor not in contents:
        raise ProvisioningError(
            "Unable to locate the pmu-gateway Compose include for processor insertion"
        )
    compose_path.write_text(contents.replace(anchor, f"{anchor}{include}"), encoding="utf-8")


def _update_processor_documentation(repository_root: Path) -> None:
    services = _processor_service_names(repository_root)
    _replace_marked_block(
        repository_root / "README.md",
        PROCESSOR_TABLE_START,
        PROCESSOR_TABLE_END,
        "\n".join(
            f"- [services/{service}/](services/{service}/) - `{service}`: "
            "editable Quixstreams processor service and tests"
            for service in services
        ),
    )
    _replace_marked_block(
        repository_root / "services" / "README.md",
        PROCESSOR_TABLE_START,
        PROCESSOR_TABLE_END,
        "\n".join(
            f"- [`{service}/`]({service}/) - `{service}`" for service in services
        ),
    )
    _replace_marked_block(
        repository_root / ".github" / "copilot-instructions.md",
        PROCESSOR_INSTRUCTIONS_START,
        PROCESSOR_INSTRUCTIONS_END,
        "\n".join(f"- `{service}`" for service in services),
    )


def _processor_service_names(repository_root: Path) -> list[str]:
    services_directory = repository_root / "services"
    return sorted(
        path.name
        for path in services_directory.iterdir()
        if path.is_dir() and path.name.startswith("processor-")
    )


def _replace_marked_block(
    path: Path,
    start_marker: str,
    end_marker: str,
    replacement: str,
) -> None:
    contents = path.read_text(encoding="utf-8")
    start = contents.find(start_marker)
    end = contents.find(end_marker)
    if start == -1 or end == -1 or end < start:
        raise ProvisioningError(f"Missing processor documentation markers in {path}")

    block_start = start + len(start_marker)
    updated = f"{contents[:block_start]}\n{replacement}\n{contents[end:]}"
    path.write_text(updated, encoding="utf-8")


def main() -> int:
    """Run the processor provisioner as a command-line tool."""

    parser = argparse.ArgumentParser(
        description="Create a processor service from templates/quixstreams-processor",
    )
    parser.add_argument("name", help="Lowercase processor name without the processor- prefix")
    arguments = parser.parse_args()

    repository_root = Path(__file__).resolve().parents[1]
    try:
        service_name = provision_processor(repository_root, arguments.name)
    except ProvisioningError as error:
        print(f"Processor provisioning failed: {error}", file=sys.stderr)
        return 1

    print(f"Provisioned {service_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())