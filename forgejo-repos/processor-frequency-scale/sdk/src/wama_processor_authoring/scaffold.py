"""Deterministic scaffolding for independently deployable standard processors."""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
import json
from pathlib import Path
import shutil
from typing import Any

from wama_processor_authoring.cases import load_cases, report_cases, run_cases
from wama_processor_authoring.catalog import (
    load_approval_catalog,
    load_input_catalog,
    resolve_processor,
)
from wama_processor_authoring.errors import AuthoringValidationError
from wama_processor_authoring.manifest import ProcessorManifest, load_manifest


GENERATED_LOCK = ".wama-generated-files.json"
_CHECKOUT_ACTION = "https://data.forgejo.org/actions/checkout@11d5960a326750d5838078e36cf38b85af677262"


def scaffold_standard_processor(
    *,
    manifest_path: Path,
    calculation_path: Path,
    cases_path: Path,
    input_catalog_path: Path,
    approval_catalog_path: Path,
    output_directory: Path,
) -> dict[str, object]:
    """Generate the non-authored deployment surface for one standard processor."""

    manifest = load_manifest(manifest_path)
    if not manifest.is_standard:
        raise AuthoringValidationError("scaffolding is available only for formula and latest-values modes")
    resolved = resolve_processor(
        manifest,
        load_input_catalog(input_catalog_path),
        load_approval_catalog(approval_catalog_path),
    )
    calculations = _load_calculations(calculation_path, manifest)
    report = report_cases(run_cases(resolved, calculations, load_cases(cases_path)))
    if not report["passed"]:
        raise AuthoringValidationError("engineering cases must pass before scaffolding")
    if output_directory.exists() and any(output_directory.iterdir()):
        raise AuthoringValidationError("scaffold output directory must be absent or empty")
    if output_directory.name != manifest.name:
        raise AuthoringValidationError("scaffold output directory name must equal manifest name")

    output_directory.mkdir(parents=True, exist_ok=True)
    package_name = manifest.name.replace("-", "_")
    authored_files = {
        "processor.yaml": manifest_path,
        "cases.yaml": cases_path,
        f"src/{package_name}/calculation.py": calculation_path,
    }
    for relative_path, source in authored_files.items():
        _copy_regular_file(source, output_directory / relative_path)

    template_root = _template_root()
    _copy_tree(template_root / "runtime", output_directory / "runtime")
    _copy_tree(template_root / "contracts", output_directory / "contracts")
    _copy_tree(template_root / "tooling-tests", output_directory / "tooling-tests")
    _copy_tree(template_root / "sdk", output_directory / "sdk")
    _write(
        output_directory / "Dockerfile",
        _replace_package(
            (template_root / "Dockerfile").read_text(encoding="utf-8"),
            package_name,
        ),
    )
    _copy_regular_file(template_root / "requirements.txt", output_directory / "requirements.txt")
    _copy_regular_file(template_root / ".gitignore", output_directory / ".gitignore")
    _write(output_directory / "compose.yaml", _compose_text(manifest))
    _write(output_directory / ".forgejo/workflows/processor.yaml", _workflow_text(manifest))
    _write(
        output_directory / "scripts/deploy_processor.py",
        _replace_service(
            (template_root / "scripts/deploy_processor.py").read_text(encoding="utf-8"),
            manifest.name,
        ),
    )
    _write(
        output_directory / "tooling-tests/test_deploy_processor.py",
        _replace_service(
            (template_root / "tooling-tests/test_deploy_processor.py").read_text(encoding="utf-8"),
            manifest.name,
        ),
    )
    _write(output_directory / f"src/{package_name}/__init__.py", "")
    _write(output_directory / f"src/{package_name}/main.py", _main_text(package_name))
    _write(output_directory / f"src/{package_name}/processor.py", _processor_text(manifest, package_name))
    _write(output_directory / "tests/__init__.py", "")
    _write(output_directory / "tests/test_generated.py", _test_text(manifest, package_name))
    _write(output_directory / "README.md", _readme_text(manifest, package_name))
    _write_generated_lock(
        output_directory,
        {*authored_files, "README.md"},
    )
    return {
        "directory": str(output_directory),
        "kind": manifest.kind,
        "name": manifest.name,
        "report": report,
    }


def verify_generated_files(directory: Path) -> dict[str, object]:
    """Reject modifications to generator-owned files in one processor checkout."""

    lock_path = directory / GENERATED_LOCK
    if lock_path.is_symlink() or not lock_path.is_file():
        raise AuthoringValidationError("generated file lock is missing")
    try:
        raw = json.loads(lock_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise AuthoringValidationError("generated file lock is invalid") from error
    if not isinstance(raw, Mapping) or raw.get("version") != 1:
        raise AuthoringValidationError("generated file lock schema is unsupported")
    files = raw.get("files")
    if not isinstance(files, Mapping) or not files:
        raise AuthoringValidationError("generated file lock contains no files")
    for relative_path, expected_digest in files.items():
        if not isinstance(relative_path, str) or not isinstance(expected_digest, str):
            raise AuthoringValidationError("generated file lock contains invalid file metadata")
        target = _safe_child(directory, relative_path)
        if target.is_symlink() or not target.is_file():
            raise AuthoringValidationError(f"generated file is missing or unsafe: {relative_path}")
        actual_digest = _file_digest(target)
        if actual_digest != expected_digest:
            raise AuthoringValidationError(f"generated file was modified: {relative_path}")
    return {"directory": str(directory), "generated_files": len(files), "passed": True}


def lock_generated_files(
    directory: Path,
    author_owned_paths: tuple[str, ...],
) -> dict[str, object]:
    """Create a generated-file lock while excluding explicit author-owned paths."""

    if directory.is_symlink() or not directory.is_dir():
        raise AuthoringValidationError("generated lock directory must be a real directory")
    author_owned = {_validated_relative_path(path) for path in author_owned_paths}
    _write_generated_lock(directory, author_owned)
    return verify_generated_files(directory)


def _load_calculations(path: Path, manifest: ProcessorManifest) -> dict[str, object]:
    import importlib.util

    if path.is_symlink() or not path.is_file():
        raise AuthoringValidationError("calculation module must be a regular file")
    specification = importlib.util.spec_from_file_location("wama_scaffold_calculation", path)
    if specification is None or specification.loader is None:
        raise AuthoringValidationError("cannot load calculation module")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    calculations: dict[str, object] = {}
    for output_name in manifest.outputs:
        calculation = getattr(module, output_name, None)
        if not callable(calculation):
            raise AuthoringValidationError(
                f"calculation module must define callable {output_name}()"
            )
        calculations[output_name] = calculation
    return calculations


def _template_root() -> Path:
    root = Path(__file__).resolve().parents[2] / "templates" / "standard"
    if root.is_symlink() or not root.is_dir():
        raise AuthoringValidationError("standard processor template bundle is unavailable")
    return root


def _copy_tree(source: Path, destination: Path) -> None:
    if source.is_symlink() or not source.is_dir():
        raise AuthoringValidationError(f"template directory is unavailable: {source.name}")
    shutil.copytree(source, destination, symlinks=False)


def _copy_regular_file(source: Path, destination: Path) -> None:
    if source.is_symlink() or not source.is_file():
        raise AuthoringValidationError(f"required regular file is unavailable: {source}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, destination, follow_symlinks=False)


def _write(path: Path, contents: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(contents, encoding="utf-8")


def _write_generated_lock(directory: Path, author_owned: set[str]) -> None:
    generated: dict[str, str] = {}
    for path in sorted(directory.rglob("*")):
        if path.is_dir() or path.name == GENERATED_LOCK:
            continue
        relative = path.relative_to(directory).as_posix()
        if _is_author_owned(relative, author_owned):
            continue
        if path.is_symlink() or not path.is_file():
            raise AuthoringValidationError(f"generated output is unsafe: {relative}")
        generated[relative] = _file_digest(path)
    _write(
        directory / GENERATED_LOCK,
        json.dumps({"version": 1, "files": generated}, indent=2, sort_keys=True) + "\n",
    )


def _processor_text(manifest: ProcessorManifest, package_name: str) -> str:
    inputs = _mapping_literal(
        {name: resolved for name, resolved in _resolved_input_mrids(manifest).items()},
        4,
    )
    outputs = _mapping_literal(
        {name: declaration.mrid for name, declaration in manifest.outputs.items()},
        4,
    )
    if manifest.kind == "formula":
        input_name = next(iter(manifest.inputs))
        output_name = next(iter(manifest.outputs))
        return f'''"""Generated formula adapter; edit calculation.py, not this file."""

from __future__ import annotations

from wama_processor import ProcessorDefinition, build_formula_processor

from {package_name}.calculation import {manifest.formula_function}


INPUTS = {inputs}
OUTPUTS = {outputs}
PROCESSOR: ProcessorDefinition = build_formula_processor(
    service_name={manifest.name!r},
    inputs=INPUTS,
    outputs=OUTPUTS,
    input_name={input_name!r},
    output_name={output_name!r},
    calculation={manifest.formula_function},
)
transform = PROCESSOR.transform
'''
    group_lines = ",\n".join(
        "    LatestValuesGroup("
        f"output={group.output!r}, inputs={group.inputs!r}, maximum_age_ms={group.maximum_age_ms}),"
        for group in manifest.latest_values_groups
    )
    calculation_lines = ",\n".join(
        f"    {output_name!r}: {output_name}," for output_name in manifest.outputs
    )
    imported = ", ".join(manifest.outputs)
    return f'''"""Generated latest-values adapter; edit calculation.py, not this file."""

from __future__ import annotations

from wama_processor import LatestValuesGroup, ProcessorDefinition, build_latest_values_processor

from {package_name}.calculation import {imported}


INPUTS = {inputs}
OUTPUTS = {outputs}
GROUPS = (
{group_lines}
)
CALCULATIONS = {{
{calculation_lines}
}}


def build_processor() -> ProcessorDefinition:
    """Create an isolated ephemeral cache for one runtime instance."""

    return build_latest_values_processor(
        service_name={manifest.name!r},
        inputs=INPUTS,
        outputs=OUTPUTS,
        groups=GROUPS,
        calculations=CALCULATIONS,
    )


PROCESSOR = build_processor()
'''


def _resolved_input_mrids(manifest: ProcessorManifest) -> dict[str, str]:
    input_catalog = load_input_catalog(_template_root() / "sdk/catalog/input-catalog.yaml")
    return {name: input_catalog.signals[declaration.signal].mrid for name, declaration in manifest.inputs.items()}


def _mapping_literal(values: Mapping[str, str], indent: int) -> str:
    prefix = " " * indent
    lines = ["{"]
    lines.extend(f"{prefix}{key!r}: {value!r}," for key, value in values.items())
    lines.append("}")
    return "\n".join(lines)


def _main_text(package_name: str) -> str:
    return f'''"""Start the generated WAMA standard processor."""

from __future__ import annotations

import logging
import os

from wama_processor import run_processor

from {package_name}.processor import PROCESSOR


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    run_processor(PROCESSOR)


if __name__ == "__main__":
    main()
'''


def _test_text(manifest: ProcessorManifest, package_name: str) -> str:
    return f'''"""Generated smoke tests for the declared standard processor."""

from __future__ import annotations

import unittest

from {package_name}.processor import PROCESSOR


class GeneratedProcessorTests(unittest.TestCase):
    def test_declares_the_expected_one_service_owner(self) -> None:
        self.assertEqual(PROCESSOR.service_name, {manifest.name!r})


if __name__ == "__main__":
    unittest.main()
'''


def _compose_text(manifest: ProcessorManifest) -> str:
    return f'''name: ${{WAMA_PROCESSOR_COMPOSE_PROJECT_NAME:-wama-{manifest.name}}}

services:
  {manifest.name}:
    image: "${{WAMA_PROCESSOR_IMAGE:-wama.local/{manifest.name}:main}}"
    build:
      context: .
      dockerfile: Dockerfile
    networks:
      - wama-infra
    restart: unless-stopped

networks:
  wama-infra:
    external: true
    name: ${{WAMA_INFRA_NETWORK:-wama-infra}}
'''


def _workflow_text(manifest: ProcessorManifest) -> str:
    template_path = _template_root() / ".forgejo" / "workflows" / "processor.yaml"
    if template_path.is_symlink() or not template_path.is_file():
        raise AuthoringValidationError("standard workflow template is unavailable")
    return (
        template_path.read_text(encoding="utf-8")
        .replace("processor-frequency-scale", manifest.name)
        .replace("__PACKAGE_NAME__", manifest.name.replace("-", "_"))
    )


def _readme_text(manifest: ProcessorManifest, package_name: str) -> str:
    return f'''# {manifest.name}

This is a generated WAMA `{manifest.kind}` processor repository. The authored
surface is `processor.yaml`, `src/{package_name}/calculation.py`, and
`cases.yaml`. Generated files are recorded in `{GENERATED_LOCK}` and validated
by Forgejo before any container build or deployment.

The repository owns exactly one `{manifest.name}` service on the existing
external `wama-infra` network. Its workflow cannot deploy root infrastructure
or a gateway.
'''


def _replace_service(contents: str, service_name: str) -> str:
    return contents.replace("processor-frequency-scale", service_name)


def _replace_package(contents: str, package_name: str) -> str:
    return contents.replace("__PACKAGE_NAME__", package_name)


def _safe_child(directory: Path, relative_path: str) -> Path:
    relative = Path(relative_path)
    if relative.is_absolute() or not relative.parts or any(part in {"", ".", ".."} for part in relative.parts):
        raise AuthoringValidationError("generated file lock contains an unsafe path")
    target = directory / relative
    try:
        target.relative_to(directory)
    except ValueError as error:
        raise AuthoringValidationError("generated file lock escapes its directory") from error
    return target


def _validated_relative_path(value: str) -> str:
    path = Path(value)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise AuthoringValidationError("author-owned path must be a safe relative path")
    return path.as_posix()


def _is_author_owned(relative_path: str, author_owned: set[str]) -> bool:
    return any(
        relative_path == owned_path or relative_path.startswith(f"{owned_path}/")
        for owned_path in author_owned
    )


def _file_digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()