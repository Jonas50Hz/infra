"""Command-line entry point for local WAMA processor authoring checks."""

from __future__ import annotations

import argparse
from collections.abc import Mapping
import importlib.util
import json
from pathlib import Path
import sys
from types import ModuleType

from wama_processor_authoring.cases import load_cases, report_cases, run_cases
from wama_processor_authoring.catalog import (
    export_input_catalog_document,
    lock_catalog_revision,
    load_approval_catalog,
    load_input_catalog,
    resolve_processor,
    write_input_catalog,
)
from wama_processor_authoring.errors import AuthoringValidationError
from wama_processor_authoring.manifest import load_manifest, validate_installed_sdk
from wama_processor_authoring.scaffold import (
    lock_generated_files,
    scaffold_standard_processor,
    verify_generated_files,
)


def main(arguments: list[str] | None = None) -> int:
    """Run one deterministic authoring action and return a shell status."""

    parser = _parser()
    args = parser.parse_args(arguments)
    try:
        if args.command == "export-catalog":
            revision = write_input_catalog(args.sources, args.catalog_id, args.output)
            print(json.dumps({"catalog": str(args.output), "revision": revision}, sort_keys=True))
            return 0
        if args.command == "lock-catalog":
            revision = lock_catalog_revision(args.catalog)
            print(json.dumps({"catalog": str(args.catalog), "revision": revision}, sort_keys=True))
            return 0
        if args.command == "new":
            validate_installed_sdk(load_manifest(args.manifest))
            result = scaffold_standard_processor(
                manifest_path=args.manifest,
                calculation_path=args.calculation,
                cases_path=args.cases,
                input_catalog_path=args.input_catalog,
                approval_catalog_path=args.approval_catalog,
                output_directory=args.output,
            )
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0
        if args.command == "verify-generated":
            print(json.dumps(verify_generated_files(args.directory), sort_keys=True))
            return 0
        if args.command == "lock-generated":
            print(
                json.dumps(
                    lock_generated_files(args.directory, tuple(args.author_owned)),
                    sort_keys=True,
                )
            )
            return 0
        processor = _resolve(args)
        validate_installed_sdk(processor.manifest)
        if args.command == "validate":
            print(json.dumps(_summary(processor), indent=2, sort_keys=True))
            return 0
        calculations = _load_calculations(args.calculation, processor.manifest.outputs)
        report = report_cases(
            run_cases(processor, calculations, load_cases(args.cases))
        )
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0 if report["passed"] else 1
    except AuthoringValidationError as error:
        parser.error(str(error))
    return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="wama-processor")
    subparsers = parser.add_subparsers(dest="command", required=True)
    export_parser = subparsers.add_parser("export-catalog")
    export_parser.add_argument("--sources", type=Path, required=True)
    export_parser.add_argument("--catalog-id", required=True)
    export_parser.add_argument("--output", type=Path, required=True)
    lock_parser = subparsers.add_parser("lock-catalog")
    lock_parser.add_argument("--catalog", type=Path, required=True)
    scaffold_parser = subparsers.add_parser("new")
    scaffold_parser.add_argument("--manifest", type=Path, required=True)
    scaffold_parser.add_argument("--calculation", type=Path, required=True)
    scaffold_parser.add_argument("--cases", type=Path, required=True)
    scaffold_parser.add_argument("--input-catalog", type=Path, required=True)
    scaffold_parser.add_argument("--approval-catalog", type=Path, required=True)
    scaffold_parser.add_argument("--output", type=Path, required=True)
    generated_parser = subparsers.add_parser("verify-generated")
    generated_parser.add_argument("--directory", type=Path, required=True)
    lock_generated_parser = subparsers.add_parser("lock-generated")
    lock_generated_parser.add_argument("--directory", type=Path, required=True)
    lock_generated_parser.add_argument("--author-owned", action="append", default=[])

    for name in ("validate", "simulate"):
        command = subparsers.add_parser(name)
        command.add_argument("--manifest", type=Path, required=True)
        command.add_argument("--input-catalog", type=Path, required=True)
        command.add_argument("--approval-catalog", type=Path, required=True)
        if name == "simulate":
            command.add_argument("--calculation", type=Path, required=True)
            command.add_argument("--cases", type=Path, required=True)
    return parser


def _resolve(args: argparse.Namespace):
    return resolve_processor(
        load_manifest(args.manifest),
        load_input_catalog(args.input_catalog),
        load_approval_catalog(args.approval_catalog),
    )


def _load_calculations(path: Path, outputs: Mapping[str, object]) -> dict[str, object]:
    module = _load_module(path)
    calculations: dict[str, object] = {}
    for output_name in outputs:
        calculation = getattr(module, output_name, None)
        if not callable(calculation):
            raise AuthoringValidationError(
                f"calculation module must define callable {output_name}()"
            )
        calculations[output_name] = calculation
    return calculations


def _load_module(path: Path) -> ModuleType:
    if not path.is_file():
        raise AuthoringValidationError(f"calculation module does not exist: {path}")
    specification = importlib.util.spec_from_file_location("wama_author_calculation", path)
    if specification is None or specification.loader is None:
        raise AuthoringValidationError(f"cannot load calculation module: {path}")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def _summary(processor) -> dict[str, object]:
    return {
        "catalog": {
            "id": processor.manifest.catalog.catalog_id,
            "revision": processor.manifest.catalog.revision,
        },
        "inputs": {
            name: {
                "mrid": resolved.signal.mrid,
                "signal": resolved.signal.reference,
                "unit": resolved.signal.unit,
                "value_kind": resolved.signal.value_kind,
            }
            for name, resolved in processor.inputs.items()
        },
        "kind": processor.manifest.kind,
        "name": processor.manifest.name,
        "outputs": {
            name: {
                "approval": resolved.approval.approval,
                "mrid": resolved.approval.mrid,
                "topic": resolved.approval.topic,
                "unit": resolved.approval.unit,
                "value_kind": resolved.approval.value_kind,
            }
            for name, resolved in processor.outputs.items()
        },
        "typed_outputs": {
            name: {
                "approval": resolved.approval.approval,
                "contract": resolved.approval.contract_id,
                "protobuf_type": resolved.approval.protobuf_type,
                "topic": resolved.approval.topic,
            }
            for name, resolved in processor.typed_outputs.items()
        },
        "sdk": {
            "sha256": processor.manifest.sdk.sha256,
            "version": processor.manifest.sdk.version,
        },
    }


if __name__ == "__main__":
    raise SystemExit(main())