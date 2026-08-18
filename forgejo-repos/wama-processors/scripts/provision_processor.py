"""Create an application-owned processor from the local Quixstreams template."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import shutil
import sys

PROCESSOR_SLUG_PATTERN = re.compile(r"[a-z0-9]+(?:-[a-z0-9]+)*\Z")
COMPOSE_START = "# provisioned-processor-includes:start"
COMPOSE_END = "# provisioned-processor-includes:end"
README_START = "<!-- provisioned-processors:start -->"
README_END = "<!-- provisioned-processors:end -->"


class ProvisioningError(ValueError):
    """Raised when an application processor cannot be provisioned safely."""


def provision_processor(application_root: Path, slug: str) -> str:
    """Copy the local template and update only application-repository files."""

    _validate_slug(slug)
    service_name = f"processor-{slug}"
    package_name = service_name.replace("-", "_")
    config_source_variable = f"{package_name.upper()}_CONFIG_SOURCE"
    template_directory = application_root / "templates" / "quixstreams-processor"
    processor_directory = application_root / "processors" / service_name

    if not template_directory.is_dir():
        raise ProvisioningError(f"Processor template is missing: {template_directory}")
    if processor_directory.exists():
        raise ProvisioningError(f"Processor already exists: {processor_directory}")

    shutil.copytree(template_directory, processor_directory)
    try:
        _replace_identifiers(
            processor_directory,
            service_name,
            package_name,
            config_source_variable,
        )
        (processor_directory / "src" / "processor_template").rename(
            processor_directory / "src" / package_name
        )
        _add_include(application_root / "compose.yaml", service_name)
        _update_readme(application_root)
    except Exception:
        shutil.rmtree(processor_directory)
        raise

    return service_name


def _validate_slug(slug: str) -> None:
    if not PROCESSOR_SLUG_PATTERN.fullmatch(slug):
        raise ProvisioningError(
            "Processor name must use lowercase letters, numbers, and single hyphens"
        )


def _replace_identifiers(
    processor_directory: Path,
    service_name: str,
    package_name: str,
    config_source_variable: str,
) -> None:
    replacements = (
        ("templates/quixstreams-processor", f"processors/{service_name}"),
        ("processor-template", service_name),
        ("processor_template", package_name),
        ("PROCESSOR_TEMPLATE_CONFIG_SOURCE", config_source_variable),
    )
    for path in processor_directory.rglob("*"):
        if not path.is_file():
            continue
        contents = path.read_text(encoding="utf-8")
        for source, target in replacements:
            contents = contents.replace(source, target)
        path.write_text(contents, encoding="utf-8")


def _add_include(compose_path: Path, service_name: str) -> None:
    _replace_marked_block(
        compose_path,
        COMPOSE_START,
        COMPOSE_END,
        "\n".join(
            f"  - ./processors/{processor}/compose.yaml"
            for processor in _processor_names(compose_path.parent)
        ),
    )


def _update_readme(application_root: Path) -> None:
    _replace_marked_block(
        application_root / "README.md",
        README_START,
        README_END,
        "\n".join(
            f"- [`processors/{processor}/`](processors/{processor}/) - `{processor}`"
            for processor in _processor_names(application_root)
        ),
    )


def _processor_names(application_root: Path) -> list[str]:
    return sorted(
        path.name
        for path in (application_root / "processors").iterdir()
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
        raise ProvisioningError(f"Missing application marker in {path}")
    marker_end = start + len(start_marker)
    path.write_text(
        f"{contents[:marker_end]}\n{replacement}\n{contents[end:]}",
        encoding="utf-8",
    )


def main() -> int:
    """Run the provisioner from inside the application repository."""

    parser = argparse.ArgumentParser(description="Create an application processor")
    parser.add_argument("name", help="Lowercase processor name without processor- prefix")
    arguments = parser.parse_args()
    application_root = Path(__file__).resolve().parents[1]
    try:
        service_name = provision_processor(application_root, arguments.name)
    except ProvisioningError as error:
        print(f"Processor provisioning failed: {error}", file=sys.stderr)
        return 1
    print(f"Provisioned {service_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())