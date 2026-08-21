"""Revision-pinned source and derived-output catalogs for processor authoring."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path
from typing import Any

import yaml

from wama_processor_authoring.errors import AuthoringValidationError
from wama_processor_authoring.manifest import (
    InputDeclaration,
    OutputDeclaration,
    ProcessorManifest,
)


@dataclass(frozen=True)
class CatalogSignal:
    """One reviewed source signal usable by an authored processor."""

    reference: str
    mrid: str
    value_kind: str
    quantity: str
    unit: str


@dataclass(frozen=True)
class InputCatalog:
    """An immutable input-catalog projection with signal lookup."""

    catalog_id: str
    revision: str
    signals: dict[str, CatalogSignal]


@dataclass(frozen=True)
class ApprovedOutput:
    """A platform-approved output MRID and its public contract."""

    mrid: str
    value_kind: str
    unit: str
    topic: str
    owner: str
    approval: str


@dataclass(frozen=True)
class ApprovedTypedContract:
    """A platform-approved non-Common-Format output contract for custom code."""

    contract_id: str
    topic: str
    protobuf_type: str
    owner: str
    approval: str


@dataclass(frozen=True)
class ApprovalCatalog:
    """An immutable list of approved, unique derived-output contracts."""

    catalog_id: str
    revision: str
    outputs: dict[str, ApprovedOutput]
    typed_contracts: dict[str, ApprovedTypedContract] = field(default_factory=dict)


@dataclass(frozen=True)
class ResolvedInput:
    """A manifest input resolved to its exact runtime MRID."""

    declaration: InputDeclaration
    signal: CatalogSignal


@dataclass(frozen=True)
class ResolvedOutput:
    """A manifest output resolved to its approved contract."""

    declaration: OutputDeclaration
    approval: ApprovedOutput


@dataclass(frozen=True)
class ResolvedTypedOutput:
    """A custom manifest output resolved to an approved typed contract."""

    declaration: Any
    approval: ApprovedTypedContract


@dataclass(frozen=True)
class ResolvedProcessor:
    """A manifest whose every input and output is approved and reproducible."""

    manifest: ProcessorManifest
    inputs: dict[str, ResolvedInput]
    outputs: dict[str, ResolvedOutput]
    typed_outputs: dict[str, ResolvedTypedOutput]


def load_input_catalog(path: Path) -> InputCatalog:
    """Load a content-addressed source catalog projection."""

    document = _load_document(path, "input catalog")
    catalog_id, revision = _catalog_identity(document, "input catalog")
    _verify_content_revision(document, revision, "input catalog")
    raw_signals = document.get("signals")
    if not isinstance(raw_signals, list) or not raw_signals:
        raise AuthoringValidationError("input catalog signals must be a non-empty list")

    signals: dict[str, CatalogSignal] = {}
    mrids: set[str] = set()
    for index, raw_signal in enumerate(raw_signals):
        signal = _mapping(raw_signal, f"input catalog signals[{index}]")
        reference = _string(signal, "reference", f"input catalog signals[{index}]")
        mrid = _string(signal, "mrid", f"input catalog signals[{index}]")
        if reference in signals:
            raise AuthoringValidationError(f"input catalog repeats reference {reference!r}")
        if mrid in mrids:
            raise AuthoringValidationError(f"input catalog repeats MRID {mrid!r}")
        mrids.add(mrid)
        signals[reference] = CatalogSignal(
            reference=reference,
            mrid=mrid,
            value_kind=_string(signal, "value_kind", f"input catalog signals[{index}]") ,
            quantity=_string(signal, "quantity", f"input catalog signals[{index}]") ,
            unit=_string(signal, "unit", f"input catalog signals[{index}]") ,
        )
    return InputCatalog(catalog_id=catalog_id, revision=revision, signals=signals)


def export_input_catalog_document(
    sources_directory: Path,
    catalog_id: str,
) -> dict[str, object]:
    """Flatten reviewed C37.118 source YAML into an authoring catalog snapshot."""

    if not isinstance(catalog_id, str) or not catalog_id.strip():
        raise AuthoringValidationError("catalog id must be a non-empty string")
    if not sources_directory.is_dir():
        raise AuthoringValidationError(
            f"source catalog directory does not exist: {sources_directory}"
        )

    signals: list[dict[str, str]] = []
    seen_references: set[str] = set()
    seen_mrids: set[str] = set()
    for source_path in sorted(sources_directory.glob("*.yaml")):
        source = _load_document(source_path, "source catalog entry")
        source_id = _string(source, "source_id", str(source_path))
        raw_signals = source.get("signals")
        if not isinstance(raw_signals, list) or not raw_signals:
            raise AuthoringValidationError(f"{source_path}.signals must be a non-empty list")
        for index, raw_signal in enumerate(raw_signals):
            signal = _mapping(raw_signal, f"{source_path}.signals[{index}]")
            signal_id = _string(signal, "signal_id", f"{source_path}.signals[{index}]")
            reference = f"{source_id}.{signal_id}"
            mrid = _string(signal, "mrid", f"{source_path}.signals[{index}]")
            if reference in seen_references:
                raise AuthoringValidationError(f"source catalog repeats reference {reference!r}")
            if mrid in seen_mrids:
                raise AuthoringValidationError(f"source catalog repeats MRID {mrid!r}")
            seen_references.add(reference)
            seen_mrids.add(mrid)
            signals.append(
                {
                    "reference": reference,
                    "mrid": mrid,
                    "value_kind": _string(
                        signal,
                        "value_kind",
                        f"{source_path}.signals[{index}]",
                    ),
                    "quantity": _string(
                        signal,
                        "quantity",
                        f"{source_path}.signals[{index}]",
                    ),
                    "unit": _string(signal, "unit", f"{source_path}.signals[{index}]"),
                }
            )
    if not signals:
        raise AuthoringValidationError("source catalog must contain at least one YAML source")
    document: dict[str, object] = {
        "catalog_id": catalog_id.strip(),
        "signals": sorted(signals, key=lambda signal: signal["reference"]),
    }
    document["revision"] = content_revision(document)
    return document


def write_input_catalog(
    sources_directory: Path,
    catalog_id: str,
    output_path: Path,
) -> str:
    """Write a stable, reviewed input-catalog snapshot and return its revision."""

    document = export_input_catalog_document(sources_directory, catalog_id)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        yaml.safe_dump(document, allow_unicode=False, sort_keys=False),
        encoding="utf-8",
    )
    return str(document["revision"])


def lock_catalog_revision(path: Path) -> str:
    """Recalculate and persist a content-addressed revision for a reviewed catalog."""

    document = dict(_load_document(path, "catalog"))
    document["revision"] = content_revision(document)
    path.write_text(
        yaml.safe_dump(document, allow_unicode=False, sort_keys=False),
        encoding="utf-8",
    )
    return str(document["revision"])


def load_approval_catalog(path: Path) -> ApprovalCatalog:
    """Load a content-addressed derived-output approval catalog."""

    document = _load_document(path, "approval catalog")
    catalog_id, revision = _catalog_identity(document, "approval catalog")
    _verify_content_revision(document, revision, "approval catalog")
    raw_outputs = document.get("outputs")
    if not isinstance(raw_outputs, list) or not raw_outputs:
        raise AuthoringValidationError("approval catalog outputs must be a non-empty list")

    outputs: dict[str, ApprovedOutput] = {}
    for index, raw_output in enumerate(raw_outputs):
        output = _mapping(raw_output, f"approval catalog outputs[{index}]")
        mrid = _string(output, "mrid", f"approval catalog outputs[{index}]")
        if mrid in outputs:
            raise AuthoringValidationError(f"approval catalog repeats MRID {mrid!r}")
        outputs[mrid] = ApprovedOutput(
            mrid=mrid,
            value_kind=_string(output, "value", f"approval catalog outputs[{index}]") ,
            unit=_string(output, "unit", f"approval catalog outputs[{index}]") ,
            topic=_string(output, "topic", f"approval catalog outputs[{index}]") ,
            owner=_string(output, "owner", f"approval catalog outputs[{index}]") ,
            approval=_string(output, "approval", f"approval catalog outputs[{index}]") ,
        )
    raw_typed_contracts = document.get("typed_contracts", [])
    if not isinstance(raw_typed_contracts, list):
        raise AuthoringValidationError("approval catalog typed_contracts must be a list")
    typed_contracts: dict[str, ApprovedTypedContract] = {}
    for index, raw_contract in enumerate(raw_typed_contracts):
        contract = _mapping(raw_contract, f"approval catalog typed_contracts[{index}]")
        contract_id = _string(contract, "id", f"approval catalog typed_contracts[{index}]")
        if contract_id in typed_contracts:
            raise AuthoringValidationError(
                f"approval catalog repeats typed contract {contract_id!r}"
            )
        typed_contracts[contract_id] = ApprovedTypedContract(
            contract_id=contract_id,
            topic=_string(contract, "topic", f"approval catalog typed_contracts[{index}]") ,
            protobuf_type=_string(
                contract,
                "protobuf_type",
                f"approval catalog typed_contracts[{index}]",
            ),
            owner=_string(contract, "owner", f"approval catalog typed_contracts[{index}]") ,
            approval=_string(
                contract,
                "approval",
                f"approval catalog typed_contracts[{index}]",
            ),
        )
    return ApprovalCatalog(
        catalog_id=catalog_id,
        revision=revision,
        outputs=outputs,
        typed_contracts=typed_contracts,
    )


def resolve_processor(
    manifest: ProcessorManifest,
    input_catalog: InputCatalog,
    approval_catalog: ApprovalCatalog,
) -> ResolvedProcessor:
    """Resolve every manifest signal against immutable reviewed evidence."""

    if manifest.catalog.catalog_id != input_catalog.catalog_id:
        raise AuthoringValidationError(
            f"manifest catalog id {manifest.catalog.catalog_id!r} does not match "
            f"input catalog {input_catalog.catalog_id!r}"
        )
    if manifest.catalog.revision != input_catalog.revision:
        raise AuthoringValidationError("manifest catalog revision does not match input catalog")
    if manifest.approvals.catalog_id != approval_catalog.catalog_id:
        raise AuthoringValidationError(
            f"manifest approval catalog id {manifest.approvals.catalog_id!r} does not match "
            f"approval catalog {approval_catalog.catalog_id!r}"
        )
    if manifest.approvals.revision != approval_catalog.revision:
        raise AuthoringValidationError(
            "manifest approval catalog revision does not match approval catalog"
        )

    resolved_inputs: dict[str, ResolvedInput] = {}
    for name, declaration in manifest.inputs.items():
        signal = input_catalog.signals.get(declaration.signal)
        if signal is None:
            raise AuthoringValidationError(
                f"inputs.{name}.signal {declaration.signal!r} is absent from the input catalog"
            )
        if signal.value_kind != declaration.expected_value:
            raise AuthoringValidationError(
                f"inputs.{name} expects {declaration.expected_value}, catalog declares {signal.value_kind}"
            )
        if signal.unit != declaration.expected_unit:
            raise AuthoringValidationError(
                f"inputs.{name} expects unit {declaration.expected_unit!r}, catalog declares {signal.unit!r}"
            )
        resolved_inputs[name] = ResolvedInput(declaration=declaration, signal=signal)

    input_mrids = {resolved.signal.mrid for resolved in resolved_inputs.values()}
    resolved_outputs: dict[str, ResolvedOutput] = {}
    for name, declaration in manifest.outputs.items():
        if declaration.mrid in input_mrids:
            raise AuthoringValidationError(f"outputs.{name} reuses an input MRID")
        approval = approval_catalog.outputs.get(declaration.mrid)
        if approval is None:
            raise AuthoringValidationError(f"outputs.{name}.mrid is not platform approved")
        if approval.value_kind != declaration.value:
            raise AuthoringValidationError(f"outputs.{name} value kind differs from its approval")
        if approval.unit != declaration.unit:
            raise AuthoringValidationError(f"outputs.{name} unit differs from its approval")
        if approval.topic != declaration.topic:
            raise AuthoringValidationError(f"outputs.{name} topic differs from its approval")
        if approval.approval != declaration.approval:
            raise AuthoringValidationError(f"outputs.{name} approval reference differs from catalog")
        if approval.owner != manifest.name:
            raise AuthoringValidationError(f"outputs.{name} is not owned by {manifest.name}")
        resolved_outputs[name] = ResolvedOutput(declaration=declaration, approval=approval)
    resolved_typed_outputs: dict[str, ResolvedTypedOutput] = {}
    for name, declaration in manifest.typed_outputs.items():
        approval = approval_catalog.typed_contracts.get(declaration.contract_id)
        if approval is None:
            raise AuthoringValidationError(
                f"typed_outputs.{name}.contract is not platform approved"
            )
        if approval.topic != declaration.topic:
            raise AuthoringValidationError(f"typed_outputs.{name} topic differs from its approval")
        if approval.protobuf_type != declaration.protobuf_type:
            raise AuthoringValidationError(
                f"typed_outputs.{name} protobuf type differs from its approval"
            )
        if approval.approval != declaration.approval:
            raise AuthoringValidationError(
                f"typed_outputs.{name} approval reference differs from catalog"
            )
        if approval.owner != manifest.name:
            raise AuthoringValidationError(
                f"typed_outputs.{name} is not owned by {manifest.name}"
            )
        resolved_typed_outputs[name] = ResolvedTypedOutput(
            declaration=declaration,
            approval=approval,
        )
    return ResolvedProcessor(
        manifest=manifest,
        inputs=resolved_inputs,
        outputs=resolved_outputs,
        typed_outputs=resolved_typed_outputs,
    )


def content_revision(document: Mapping[str, object]) -> str:
    """Return the deterministic digest required in a catalog's revision field."""

    canonical_document = {key: value for key, value in document.items() if key != "revision"}
    encoded = json.dumps(
        canonical_document,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _load_document(path: Path, label: str) -> Mapping[str, object]:
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise AuthoringValidationError(f"Unable to read {label}: {path}") from error
    except yaml.YAMLError as error:
        raise AuthoringValidationError(f"Invalid YAML in {label}: {path}") from error
    return _mapping(raw, label)


def _catalog_identity(document: Mapping[str, object], label: str) -> tuple[str, str]:
    catalog_id = _string(document, "catalog_id", label)
    revision = _string(document, "revision", label)
    if not revision.startswith("sha256:") or len(revision) != len("sha256:") + 64:
        raise AuthoringValidationError(f"{label} revision must be a sha256 content digest")
    return catalog_id, revision


def _verify_content_revision(
    document: Mapping[str, object],
    revision: str,
    label: str,
) -> None:
    if content_revision(document) != revision:
        raise AuthoringValidationError(f"{label} revision does not match its content")


def _mapping(value: object, location: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise AuthoringValidationError(f"{location} must be a mapping")
    return value


def _string(document: Mapping[str, object], field: str, location: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value.strip():
        raise AuthoringValidationError(f"{location}.{field} must be a non-empty string")
    return value.strip()