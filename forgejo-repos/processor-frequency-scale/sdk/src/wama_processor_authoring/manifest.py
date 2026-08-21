"""Strict parsing for the v1alpha1 WAMA processor manifest."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
from typing import Any

import yaml

from wama_processor_authoring.errors import AuthoringValidationError


API_VERSION = "wama.processor/v1alpha1"
STANDARD_KINDS = frozenset({"formula", "latest-values", "custom"})
_LOCAL_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")
_SLUG_IDENTIFIER = re.compile(r"^[a-z][a-z0-9-]*$")
_SHA256 = re.compile(r"^sha256:[0-9a-f]{64}$")


@dataclass(frozen=True)
class SdkLock:
    """The immutable SDK release used to generate and run a processor."""

    version: str
    sha256: str


@dataclass(frozen=True)
class CatalogReference:
    """The reviewed input-catalog snapshot used by a manifest."""

    catalog_id: str
    revision: str


@dataclass(frozen=True)
class ApprovalReference:
    """The reviewed derived-output approval catalog used by a manifest."""

    catalog_id: str
    revision: str


@dataclass(frozen=True)
class InputDeclaration:
    """An author-facing name bound to one reviewed source signal."""

    name: str
    signal: str
    expected_value: str
    expected_unit: str


@dataclass(frozen=True)
class OutputDeclaration:
    """An approved derived output owned by this processor."""

    name: str
    mrid: str
    value: str
    unit: str
    approval: str
    topic: str


@dataclass(frozen=True)
class TypedOutputDeclaration:
    """A custom processor's explicitly approved typed output contract."""

    name: str
    contract_id: str
    topic: str
    protobuf_type: str
    approval: str


@dataclass(frozen=True)
class LatestValuesGroup:
    """One complete latest-values group that emits a declared output."""

    output: str
    inputs: tuple[str, ...]
    maximum_age_ms: int


@dataclass(frozen=True)
class ProcessorManifest:
    """The constrained processor declaration shared by tooling and runtime."""

    api_version: str
    kind: str
    name: str
    service_name: str
    sdk: SdkLock
    catalog: CatalogReference
    approvals: ApprovalReference
    inputs: dict[str, InputDeclaration]
    outputs: dict[str, OutputDeclaration]
    typed_outputs: dict[str, TypedOutputDeclaration]
    formula_function: str | None
    latest_values_groups: tuple[LatestValuesGroup, ...]
    custom_entrypoint: str | None

    @property
    def is_standard(self) -> bool:
        """Return whether this manifest uses a generated standard adapter."""

        return self.kind in {"formula", "latest-values"}


def installed_sdk_digest() -> str:
    """Return a stable content digest of the installed authoring SDK modules."""

    package_directory = Path(__file__).parent
    digest = hashlib.sha256()
    for module_path in sorted(package_directory.glob("*.py")):
        if module_path.is_symlink() or not module_path.is_file():
            raise AuthoringValidationError("installed SDK contains an unsafe module path")
        digest.update(module_path.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(hashlib.sha256(module_path.read_bytes()).digest())
    return f"sha256:{digest.hexdigest()}"


def validate_installed_sdk(manifest: ProcessorManifest) -> None:
    """Reject a manifest that does not pin the SDK actually running validation."""

    actual_digest = installed_sdk_digest()
    if manifest.sdk.sha256 != actual_digest:
        raise AuthoringValidationError(
            f"manifest SDK digest {manifest.sdk.sha256} does not match installed SDK {actual_digest}"
        )


def load_manifest(path: Path) -> ProcessorManifest:
    """Load and validate a manifest without importing author calculation code."""

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise AuthoringValidationError(f"Unable to read manifest: {path}") from error
    except yaml.YAMLError as error:
        raise AuthoringValidationError(f"Invalid YAML in manifest: {path}") from error
    return parse_manifest(raw)


def parse_manifest(raw: object) -> ProcessorManifest:
    """Parse an in-memory manifest and reject ambiguous or unsafe declarations."""

    document = _mapping(raw, "processor manifest")
    api_version = _required_string(document, "api_version")
    if api_version != API_VERSION:
        raise AuthoringValidationError(
            f"api_version must be {API_VERSION!r}; found {api_version!r}"
        )
    kind = _required_string(document, "kind")
    if kind not in STANDARD_KINDS:
        raise AuthoringValidationError(f"kind must be one of {sorted(STANDARD_KINDS)}")

    name = _processor_name(_required_string(document, "name"), "name")
    service_name = _processor_name(
        _optional_string(document, "service_name", name),
        "service_name",
    )
    if service_name != name:
        raise AuthoringValidationError("service_name must equal name for one-service ownership")

    sdk = _parse_sdk(_mapping(document.get("sdk"), "sdk"))
    catalog = _parse_catalog(_mapping(document.get("catalog"), "catalog"))
    approvals = _parse_approval_catalog(_mapping(document.get("approvals"), "approvals"))

    typed_outputs: dict[str, TypedOutputDeclaration] = {}
    formula_function: str | None = None
    latest_values_groups: tuple[LatestValuesGroup, ...] = ()
    custom_entrypoint: str | None = None
    if kind == "custom":
        inputs = _parse_inputs(
            _mapping(document.get("inputs", {}), "inputs"),
            allow_empty=True,
        )
        outputs = _parse_outputs(
            _mapping(document.get("outputs", {}), "outputs"),
            allow_empty=True,
        )
        typed_outputs = _parse_typed_outputs(
            _mapping(document.get("typed_outputs", {}), "typed_outputs"),
            allow_empty=True,
        )
        if not outputs and not typed_outputs:
            raise AuthoringValidationError(
                "custom mode must declare at least one approved output contract"
            )
        if set(outputs).intersection(typed_outputs):
            raise AuthoringValidationError("output names must not overlap typed output names")
        custom_entrypoint = _required_string(document, "entrypoint")
    else:
        inputs = _parse_inputs(_mapping(document.get("inputs"), "inputs"))
        outputs = _parse_outputs(_mapping(document.get("outputs"), "outputs"))
        if kind == "formula":
            formula_function = _parse_formula(document, inputs, outputs)
        else:
            latest_values_groups = _parse_latest_values(document, inputs, outputs)
    _validate_distinct_mrids(inputs, outputs)

    return ProcessorManifest(
        api_version=api_version,
        kind=kind,
        name=name,
        service_name=service_name,
        sdk=sdk,
        catalog=catalog,
        approvals=approvals,
        inputs=inputs,
        outputs=outputs,
        typed_outputs=typed_outputs,
        formula_function=formula_function,
        latest_values_groups=latest_values_groups,
        custom_entrypoint=custom_entrypoint,
    )


def _parse_sdk(raw: Mapping[str, object]) -> SdkLock:
    version = _required_string(raw, "version")
    sha256 = _required_string(raw, "sha256")
    _sha256(sha256, "sdk.sha256")
    return SdkLock(version=version, sha256=sha256)


def _parse_catalog(raw: Mapping[str, object]) -> CatalogReference:
    catalog_id = _required_string(raw, "id")
    revision = _required_string(raw, "revision")
    _sha256(revision, "catalog.revision")
    return CatalogReference(catalog_id=catalog_id, revision=revision)


def _parse_approval_catalog(raw: Mapping[str, object]) -> ApprovalReference:
    catalog_id = _required_string(raw, "id")
    revision = _required_string(raw, "revision")
    _sha256(revision, "approvals.revision")
    return ApprovalReference(catalog_id=catalog_id, revision=revision)


def _parse_inputs(
    raw: Mapping[str, object],
    *,
    allow_empty: bool = False,
) -> dict[str, InputDeclaration]:
    if not raw and not allow_empty:
        raise AuthoringValidationError("inputs must declare at least one signal")
    inputs: dict[str, InputDeclaration] = {}
    for name, value in raw.items():
        input_name = _identifier(name, "input name")
        declaration = _mapping(value, f"inputs.{input_name}")
        expected_value = _required_string(declaration, "expected_value")
        if expected_value != "double":
            raise AuthoringValidationError(
                f"inputs.{input_name}.expected_value must be 'double' in v1alpha1"
            )
        inputs[input_name] = InputDeclaration(
            name=input_name,
            signal=_signal_reference(_required_string(declaration, "signal"), input_name),
            expected_value=expected_value,
            expected_unit=_required_string(declaration, "expected_unit"),
        )
    return inputs


def _parse_outputs(
    raw: Mapping[str, object],
    *,
    allow_empty: bool = False,
) -> dict[str, OutputDeclaration]:
    if not raw and not allow_empty:
        raise AuthoringValidationError("outputs must declare at least one signal")
    outputs: dict[str, OutputDeclaration] = {}
    mrids: set[str] = set()
    for name, value in raw.items():
        output_name = _identifier(name, "output name")
        declaration = _mapping(value, f"outputs.{output_name}")
        mrid = _required_string(declaration, "mrid")
        if mrid in mrids:
            raise AuthoringValidationError("output MRIDs must be unique")
        mrids.add(mrid)
        output_value = _required_string(declaration, "value")
        if output_value != "double":
            raise AuthoringValidationError(
                f"outputs.{output_name}.value must be 'double' in v1alpha1"
            )
        outputs[output_name] = OutputDeclaration(
            name=output_name,
            mrid=mrid,
            value=output_value,
            unit=_required_string(declaration, "unit"),
            approval=_required_string(declaration, "approval"),
            topic=_optional_string(declaration, "topic", "LiveMeasurement"),
        )
    return outputs


def _parse_typed_outputs(
    raw: Mapping[str, object],
    *,
    allow_empty: bool = False,
) -> dict[str, TypedOutputDeclaration]:
    if not raw and not allow_empty:
        raise AuthoringValidationError("typed_outputs must declare at least one output")
    outputs: dict[str, TypedOutputDeclaration] = {}
    contract_ids: set[str] = set()
    for name, value in raw.items():
        output_name = _identifier(name, "typed output name")
        declaration = _mapping(value, f"typed_outputs.{output_name}")
        contract_id = _required_string(declaration, "contract")
        if contract_id in contract_ids:
            raise AuthoringValidationError("typed output contracts must be unique")
        contract_ids.add(contract_id)
        outputs[output_name] = TypedOutputDeclaration(
            name=output_name,
            contract_id=contract_id,
            topic=_required_string(declaration, "topic"),
            protobuf_type=_required_string(declaration, "protobuf_type"),
            approval=_required_string(declaration, "approval"),
        )
    return outputs


def _validate_distinct_mrids(
    inputs: Mapping[str, InputDeclaration],
    outputs: Mapping[str, OutputDeclaration],
) -> None:
    signals = {declaration.signal for declaration in inputs.values()}
    output_mrids = {declaration.mrid for declaration in outputs.values()}
    if signals.intersection(output_mrids):
        raise AuthoringValidationError("outputs must not reuse a declared input signal reference")


def _parse_formula(
    document: Mapping[str, object],
    inputs: Mapping[str, InputDeclaration],
    outputs: Mapping[str, OutputDeclaration],
) -> str:
    if len(inputs) != 1 or len(outputs) != 1:
        raise AuthoringValidationError("formula mode requires exactly one input and one output")
    calculation = _mapping(document.get("calculation"), "calculation")
    function = _identifier(_required_string(calculation, "function"), "calculation.function")
    output_name = next(iter(outputs))
    if function != output_name:
        raise AuthoringValidationError(
            "calculation.function must match the sole declared formula output"
        )
    return function


def _parse_latest_values(
    document: Mapping[str, object],
    inputs: Mapping[str, InputDeclaration],
    outputs: Mapping[str, OutputDeclaration],
) -> tuple[LatestValuesGroup, ...]:
    latest_values = _mapping(document.get("latest_values"), "latest_values")
    raw_groups = latest_values.get("groups")
    if not isinstance(raw_groups, list) or not raw_groups:
        raise AuthoringValidationError("latest_values.groups must be a non-empty list")

    groups: list[LatestValuesGroup] = []
    grouped_inputs: set[str] = set()
    grouped_outputs: set[str] = set()
    for index, raw_group in enumerate(raw_groups):
        group = _mapping(raw_group, f"latest_values.groups[{index}]")
        output = _identifier(_required_string(group, "output"), f"latest_values.groups[{index}].output")
        if output not in outputs:
            raise AuthoringValidationError(
                f"latest_values.groups[{index}].output must name a declared output"
            )
        if output in grouped_outputs:
            raise AuthoringValidationError("latest_values groups must not share an output")
        raw_inputs = group.get("inputs")
        if not isinstance(raw_inputs, list) or len(raw_inputs) < 2:
            raise AuthoringValidationError(
                f"latest_values.groups[{index}].inputs must list at least two inputs"
            )
        group_inputs = tuple(
            _identifier(input_name, f"latest_values.groups[{index}].inputs")
            for input_name in raw_inputs
        )
        if len(set(group_inputs)) != len(group_inputs):
            raise AuthoringValidationError("latest_values group inputs must be unique")
        unknown_inputs = set(group_inputs).difference(inputs)
        if unknown_inputs:
            raise AuthoringValidationError(
                f"latest_values group names undeclared inputs: {', '.join(sorted(unknown_inputs))}"
            )
        shared_inputs = grouped_inputs.intersection(group_inputs)
        if shared_inputs:
            names = ", ".join(sorted(shared_inputs))
            raise AuthoringValidationError(
                f"latest_values inputs must belong to exactly one group: {names}"
            )
        maximum_age_ms = group.get("maximum_age_ms")
        if isinstance(maximum_age_ms, bool) or not isinstance(maximum_age_ms, int):
            raise AuthoringValidationError(
                f"latest_values.groups[{index}].maximum_age_ms must be an integer"
            )
        if maximum_age_ms <= 0:
            raise AuthoringValidationError(
                f"latest_values.groups[{index}].maximum_age_ms must be positive"
            )
        grouped_outputs.add(output)
        grouped_inputs.update(group_inputs)
        groups.append(
            LatestValuesGroup(
                output=output,
                inputs=group_inputs,
                maximum_age_ms=maximum_age_ms,
            )
        )

    if grouped_inputs != set(inputs):
        missing = ", ".join(sorted(set(inputs).difference(grouped_inputs)))
        raise AuthoringValidationError(f"latest_values inputs must belong to a group: {missing}")
    if grouped_outputs != set(outputs):
        missing = ", ".join(sorted(set(outputs).difference(grouped_outputs)))
        raise AuthoringValidationError(f"latest_values outputs must have a group: {missing}")
    return tuple(groups)


def _mapping(value: object, location: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise AuthoringValidationError(f"{location} must be a mapping")
    return value


def _required_string(document: Mapping[str, object], field: str) -> str:
    value = document.get(field)
    if not isinstance(value, str) or not value.strip():
        raise AuthoringValidationError(f"{field} must be a non-empty string")
    return value.strip()


def _optional_string(document: Mapping[str, object], field: str, default: str) -> str:
    if field not in document:
        return default
    return _required_string(document, field)


def _identifier(value: object, location: str) -> str:
    if not isinstance(value, str) or not _LOCAL_IDENTIFIER.fullmatch(value):
        raise AuthoringValidationError(f"{location} must be a lowercase engineering name")
    return value


def _processor_name(value: str, location: str) -> str:
    if not _SLUG_IDENTIFIER.fullmatch(value):
        raise AuthoringValidationError(f"{location} must be a lowercase processor name")
    name = value
    if not name.startswith("processor-"):
        raise AuthoringValidationError(f"{location} must begin with 'processor-'")
    return name


def _signal_reference(value: str, input_name: str) -> str:
    source_id, separator, signal_id = value.partition(".")
    if (
        not separator
        or not _SLUG_IDENTIFIER.fullmatch(source_id)
        or not _SLUG_IDENTIFIER.fullmatch(signal_id)
    ):
        raise AuthoringValidationError(
            f"inputs.{input_name}.signal must be a '<source-id>.<signal-id>' reference"
        )
    return value


def _sha256(value: str, location: str) -> None:
    if not _SHA256.fullmatch(value):
        raise AuthoringValidationError(f"{location} must be a lowercase sha256 digest")