"""Focused tests for the first WAMA processor-authoring contract slice."""

from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

import yaml

from wama_processor_authoring.catalog import (
    content_revision,
    export_input_catalog_document,
    lock_catalog_revision,
    load_input_catalog,
    resolve_processor,
)
from wama_processor_authoring.cases import report_cases, run_cases
from wama_processor_authoring.cli import main
from wama_processor_authoring.errors import AuthoringValidationError
from wama_processor_authoring.manifest import (
    installed_sdk_digest,
    parse_manifest,
    validate_installed_sdk,
)
from wama_processor_authoring.simulation import InputSample, simulate_formula
from wama_processor_authoring.simulation import LatestValuesSimulator
from wama_processor_authoring.scaffold import (
    _workflow_text,
    lock_generated_files,
    scaffold_standard_processor,
    verify_generated_files,
)


SDK_SHA = "sha256:" + "a" * 64


def input_catalog_document() -> dict[str, object]:
    document: dict[str, object] = {
        "catalog_id": "wama-c37-118-v1",
        "signals": [
            {
                "reference": "pmu-bay-01.frequency",
                "mrid": "urn:wama:poc:pmu:bay-01:frequency",
                "value_kind": "double",
                "quantity": "frequency",
                "unit": "Hz",
            }
        ],
    }
    document["revision"] = content_revision(document)
    return document


def approval_catalog_document() -> dict[str, object]:
    document: dict[str, object] = {
        "catalog_id": "wama-derived-output-v1",
        "outputs": [
            {
                "mrid": "urn:wama:poc:pmu:bay-01:frequency-millihertz",
                "value": "double",
                "unit": "mHz",
                "topic": "LiveMeasurement",
                "owner": "processor-frequency-scale",
                "approval": "WAMA-OUTPUT-001",
            },
            {
                "mrid": "urn:wama:poc:pmu:bay-01:apparent-power-l1",
                "value": "double",
                "unit": "VA",
                "topic": "LiveMeasurement",
                "owner": "processor-apparent-power",
                "approval": "WAMA-OUTPUT-002",
            },
        ],
        "typed_contracts": [
            {
                "id": "iec104-export-record",
                "topic": "Export",
                "protobuf_type": "wama.iec104.v1.ExportRecord",
                "owner": "processor-frequency-iec104-export",
                "approval": "WAMA-CONTRACT-001",
            }
        ],
    }
    document["revision"] = content_revision(document)
    return document


def formula_manifest_document() -> dict[str, object]:
    inputs = input_catalog_document()
    approvals = approval_catalog_document()
    return {
        "api_version": "wama.processor/v1alpha1",
        "kind": "formula",
        "name": "processor-frequency-scale",
        "sdk": {"version": "0.1.0", "sha256": SDK_SHA},
        "catalog": {"id": inputs["catalog_id"], "revision": inputs["revision"]},
        "approvals": {"id": approvals["catalog_id"], "revision": approvals["revision"]},
        "inputs": {
            "frequency_hz": {
                "signal": "pmu-bay-01.frequency",
                "expected_value": "double",
                "expected_unit": "Hz",
            }
        },
        "outputs": {
            "frequency_millihertz": {
                "mrid": "urn:wama:poc:pmu:bay-01:frequency-millihertz",
                "value": "double",
                "unit": "mHz",
                "approval": "WAMA-OUTPUT-001",
            }
        },
        "calculation": {"function": "frequency_millihertz"},
    }


def latest_values_manifest_document() -> dict[str, object]:
    inputs = input_catalog_document()
    inputs["signals"].extend(
        [
            {
                "reference": "pmu-bay-01.voltage-l1",
                "mrid": "urn:wama:poc:pmu:bay-01:voltage-l1",
                "value_kind": "double",
                "quantity": "voltage",
                "unit": "V",
            },
            {
                "reference": "pmu-bay-01.current-l1",
                "mrid": "urn:wama:poc:pmu:bay-01:current-l1",
                "value_kind": "double",
                "quantity": "current",
                "unit": "A",
            },
        ]
    )
    inputs["revision"] = content_revision(inputs)
    approvals = approval_catalog_document()
    return {
        "api_version": "wama.processor/v1alpha1",
        "kind": "latest-values",
        "name": "processor-apparent-power",
        "sdk": {"version": "0.1.0", "sha256": SDK_SHA},
        "catalog": {"id": inputs["catalog_id"], "revision": inputs["revision"]},
        "approvals": {"id": approvals["catalog_id"], "revision": approvals["revision"]},
        "inputs": {
            "voltage_l1": {
                "signal": "pmu-bay-01.voltage-l1",
                "expected_value": "double",
                "expected_unit": "V",
            },
            "current_l1": {
                "signal": "pmu-bay-01.current-l1",
                "expected_value": "double",
                "expected_unit": "A",
            },
        },
        "outputs": {
            "apparent_power_l1": {
                "mrid": "urn:wama:poc:pmu:bay-01:apparent-power-l1",
                "value": "double",
                "unit": "VA",
                "approval": "WAMA-OUTPUT-002",
            }
        },
        "latest_values": {
            "groups": [
                {
                    "output": "apparent_power_l1",
                    "inputs": ["voltage_l1", "current_l1"],
                    "maximum_age_ms": 2_000,
                }
            ]
        },
    }


def custom_typed_manifest_document() -> dict[str, object]:
    inputs = input_catalog_document()
    approvals = approval_catalog_document()
    return {
        "api_version": "wama.processor/v1alpha1",
        "kind": "custom",
        "name": "processor-frequency-iec104-export",
        "sdk": {"version": "0.1.0", "sha256": SDK_SHA},
        "catalog": {"id": inputs["catalog_id"], "revision": inputs["revision"]},
        "approvals": {"id": approvals["catalog_id"], "revision": approvals["revision"]},
        "inputs": {
            "frequency_hz": {
                "signal": "pmu-bay-01.frequency",
                "expected_value": "double",
                "expected_unit": "Hz",
            }
        },
        "outputs": {},
        "typed_outputs": {
            "export_record": {
                "contract": "iec104-export-record",
                "topic": "Export",
                "protobuf_type": "wama.iec104.v1.ExportRecord",
                "approval": "WAMA-CONTRACT-001",
            }
        },
        "entrypoint": "processor_frequency_iec104_export.main:main",
    }


class AuthoringTests(unittest.TestCase):
    """Prove a formula can resolve and simulate without Kafka or Docker."""

    def test_resolves_and_simulates_an_approved_formula(self) -> None:
        manifest = parse_manifest(formula_manifest_document())
        input_catalog = self._input_catalog()
        approval_catalog = self._approval_catalog()

        processor = resolve_processor(manifest, input_catalog, approval_catalog)
        result = simulate_formula(
            processor,
            lambda frequency_hz: frequency_hz * 1_000.0,
            {"frequency_hz": InputSample(value=50.01, valid=True, timestamp_ms=1_000)},
        )

        self.assertIsNone(result.reason)
        self.assertIsNotNone(result.output)
        assert result.output is not None
        self.assertEqual(result.output.name, "frequency_millihertz")
        self.assertEqual(result.output.value, 50_010.0)
        self.assertEqual(result.output.unit, "mHz")
        self.assertEqual(result.output.timestamp_ms, 1_000)

    def test_rejects_a_manifest_with_more_than_one_formula_input(self) -> None:
        document = formula_manifest_document()
        document["inputs"]["other_frequency_hz"] = {
            "signal": "pmu-bay-01.frequency",
            "expected_value": "double",
            "expected_unit": "Hz",
        }

        with self.assertRaisesRegex(AuthoringValidationError, "exactly one input"):
            parse_manifest(document)

    def test_rejects_an_unapproved_output(self) -> None:
        approvals = approval_catalog_document()
        approvals["outputs"] = [
            {
                "mrid": "urn:wama:poc:pmu:bay-01:different-output",
                "value": "double",
                "unit": "mHz",
                "topic": "LiveMeasurement",
                "owner": "wama-platform",
                "approval": "WAMA-OUTPUT-999",
            }
        ]
        approvals["revision"] = content_revision(approvals)
        manifest_document = formula_manifest_document()
        manifest_document["approvals"]["revision"] = approvals["revision"]
        manifest = parse_manifest(manifest_document)

        with self.assertRaisesRegex(AuthoringValidationError, "not platform approved"):
            resolve_processor(manifest, self._input_catalog(), self._approval_catalog(approvals))

    def test_suppresses_invalid_quality_and_non_finite_results(self) -> None:
        processor = resolve_processor(
            parse_manifest(formula_manifest_document()),
            self._input_catalog(),
            self._approval_catalog(),
        )

        invalid = simulate_formula(
            processor,
            lambda frequency_hz: frequency_hz * 1_000.0,
            {"frequency_hz": InputSample(value=50.0, valid=False, timestamp_ms=1_000)},
        )
        non_finite = simulate_formula(
            processor,
            lambda frequency_hz: float("inf"),
            {"frequency_hz": InputSample(value=50.0, valid=True, timestamp_ms=1_000)},
        )

        self.assertEqual(invalid.reason, "invalid_quality:frequency_hz")
        self.assertEqual(non_finite.reason, "non_finite_result")

    def test_rejects_a_manifest_pinned_to_a_different_installed_sdk(self) -> None:
        document = formula_manifest_document()
        document["sdk"]["sha256"] = installed_sdk_digest()
        validate_installed_sdk(parse_manifest(document))
        document["sdk"]["sha256"] = SDK_SHA

        with self.assertRaisesRegex(AuthoringValidationError, "does not match installed SDK"):
            validate_installed_sdk(parse_manifest(document))

    def test_resolves_an_approved_custom_typed_output(self) -> None:
        processor = resolve_processor(
            parse_manifest(custom_typed_manifest_document()),
            self._input_catalog(),
            self._approval_catalog(),
        )

        self.assertEqual(processor.manifest.custom_entrypoint, "processor_frequency_iec104_export.main:main")
        self.assertEqual(
            processor.typed_outputs["export_record"].approval.protobuf_type,
            "wama.iec104.v1.ExportRecord",
        )

    def test_rejects_catalog_revision_tampering(self) -> None:
        document = input_catalog_document()
        document["signals"][0]["unit"] = "kHz"

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "input-catalog.yaml"
            path.write_text(yaml.safe_dump(document), encoding="utf-8")

            with self.assertRaisesRegex(AuthoringValidationError, "does not match its content"):
                load_input_catalog(path)

    def test_latest_values_emits_in_both_arrival_orders_with_newest_timestamp(self) -> None:
        processor = self._latest_values_processor()
        calculations = {"apparent_power_l1": lambda voltage_l1, current_l1: voltage_l1 * current_l1}

        voltage_then_current = LatestValuesSimulator(processor, calculations)
        self.assertEqual(
            voltage_then_current.accept(
                "voltage_l1", InputSample(value=230.4, valid=True, timestamp_ms=1_000)
            ).reason,
            "incomplete_group:apparent_power_l1",
        )
        normal_output = voltage_then_current.accept(
            "current_l1", InputSample(value=318.2, valid=True, timestamp_ms=1_100)
        ).output

        current_then_voltage = LatestValuesSimulator(processor, calculations)
        self.assertEqual(
            current_then_voltage.accept(
                "current_l1", InputSample(value=318.2, valid=True, timestamp_ms=1_100)
            ).reason,
            "incomplete_group:apparent_power_l1",
        )
        reverse_output = current_then_voltage.accept(
            "voltage_l1", InputSample(value=230.4, valid=True, timestamp_ms=1_000)
        ).output

        self.assertIsNotNone(normal_output)
        self.assertIsNotNone(reverse_output)
        assert normal_output is not None
        assert reverse_output is not None
        self.assertEqual(normal_output.value, 73_313.28)
        self.assertEqual(reverse_output.value, 73_313.28)
        self.assertEqual(normal_output.timestamp_ms, 1_100)
        self.assertEqual(reverse_output.timestamp_ms, 1_100)

    def test_latest_values_expires_stale_state_and_loses_state_after_restart(self) -> None:
        processor = self._latest_values_processor()
        calculations = {"apparent_power_l1": lambda voltage_l1, current_l1: voltage_l1 * current_l1}
        simulator = LatestValuesSimulator(processor, calculations)

        simulator.accept(
            "voltage_l1", InputSample(value=230.4, valid=True, timestamp_ms=1_000)
        )
        stale = simulator.accept(
            "current_l1", InputSample(value=318.2, valid=True, timestamp_ms=3_001)
        )
        restarted = LatestValuesSimulator(processor, calculations)
        after_restart = restarted.accept(
            "current_l1", InputSample(value=318.2, valid=True, timestamp_ms=3_001)
        )

        self.assertEqual(stale.reason, "stale_input:voltage_l1")
        self.assertEqual(after_restart.reason, "incomplete_group:apparent_power_l1")

    def test_latest_values_replays_deterministically_and_suppresses_invalid_input(self) -> None:
        simulator = LatestValuesSimulator(
            self._latest_values_processor(),
            {"apparent_power_l1": lambda voltage_l1, current_l1: voltage_l1 * current_l1},
        )
        simulator.accept(
            "voltage_l1", InputSample(value=230.4, valid=True, timestamp_ms=1_000)
        )
        first = simulator.accept(
            "current_l1", InputSample(value=318.2, valid=True, timestamp_ms=1_100)
        )
        replay = simulator.accept(
            "current_l1", InputSample(value=318.2, valid=True, timestamp_ms=1_100)
        )
        invalid = simulator.accept(
            "current_l1", InputSample(value=318.2, valid=False, timestamp_ms=1_200)
        )

        self.assertEqual(first, replay)
        self.assertEqual(invalid.reason, "invalid_quality:current_l1")

    def test_cases_report_formula_success_and_no_output(self) -> None:
        processor = resolve_processor(
            parse_manifest(formula_manifest_document()),
            self._input_catalog(),
            self._approval_catalog(),
        )
        cases = (
            {
                "name": "nominal",
                "inputs": {
                    "frequency_hz": {"value": 50.01, "valid": True, "timestamp_ms": 1_000}
                },
                "expect": {"frequency_millihertz": {"value": 50_010.0, "unit": "mHz"}},
            },
            {
                "name": "invalid quality",
                "inputs": {
                    "frequency_hz": {"value": 50.01, "valid": False, "timestamp_ms": 1_000}
                },
                "expect": "no_output",
            },
        )

        report = report_cases(
            run_cases(
                processor,
                {"frequency_millihertz": lambda frequency_hz: frequency_hz * 1_000.0},
                cases,
            )
        )

        self.assertTrue(report["passed"])
        self.assertEqual(len(report["cases"]), 2)

    def test_exports_a_deterministic_input_catalog_from_source_entries(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            sources = Path(directory) / "sources"
            sources.mkdir()
            (sources / "pmu-bay-01.yaml").write_text(
                """source_id: pmu-bay-01
signals:
  - signal_id: frequency
    mrid: urn:wama:poc:pmu:bay-01:frequency
    value_kind: double
    quantity: frequency
    unit: Hz
""",
                encoding="utf-8",
            )

            document = export_input_catalog_document(sources, "wama-c37-118-v1")

        self.assertEqual(document["catalog_id"], "wama-c37-118-v1")
        self.assertEqual(document["signals"][0]["reference"], "pmu-bay-01.frequency")
        self.assertEqual(document["revision"], content_revision(document))

    def test_locks_a_catalog_revision_from_its_current_content(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "approval-catalog.yaml"
            path.write_text(
                """catalog_id: wama-derived-output-v1
outputs: []
revision: sha256:old
""",
                encoding="utf-8",
            )

            revision = lock_catalog_revision(path)
            document = yaml.safe_load(path.read_text(encoding="utf-8"))

        self.assertEqual(revision, content_revision(document))
        self.assertEqual(document["revision"], revision)

    def test_scaffolds_and_locks_a_standard_processor_seed(self) -> None:
        root = Path(__file__).resolve().parents[1]
        source = root.parent / "forgejo-repos" / "processor-frequency-scale"
        catalog = root / "catalog"
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "processor-frequency-scale"
            result = scaffold_standard_processor(
                manifest_path=source / "processor.yaml",
                calculation_path=source / "src/processor_frequency_scale/calculation.py",
                cases_path=source / "cases.yaml",
                input_catalog_path=catalog / "input-catalog.yaml",
                approval_catalog_path=catalog / "derived-output-approvals.yaml",
                output_directory=output,
            )

            verified = verify_generated_files(output)
            (output / "compose.yaml").write_text("services: {}\n", encoding="utf-8")

            self.assertEqual(result["name"], "processor-frequency-scale")
            self.assertTrue(verified["passed"])
            with self.assertRaisesRegex(AuthoringValidationError, "generated file was modified"):
                verify_generated_files(output)

    def test_allows_author_owned_readme_changes_in_a_scaffolded_seed(self) -> None:
        root = Path(__file__).resolve().parents[1]
        source = root.parent / "forgejo-repos" / "processor-frequency-scale"
        catalog = root / "catalog"
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "processor-frequency-scale"
            scaffold_standard_processor(
                manifest_path=source / "processor.yaml",
                calculation_path=source / "src/processor_frequency_scale/calculation.py",
                cases_path=source / "cases.yaml",
                input_catalog_path=catalog / "input-catalog.yaml",
                approval_catalog_path=catalog / "derived-output-approvals.yaml",
                output_directory=output,
            )
            (output / "README.md").write_text("Author-owned notes\n", encoding="utf-8")

            self.assertTrue(verify_generated_files(output)["passed"])

    def test_scaffolded_lifecycle_probe_uses_its_own_python_entrypoint(self) -> None:
        root = Path(__file__).resolve().parents[1]
        fixture = root / "tests" / "fixtures" / "lifecycle-probe"
        catalog = root / "catalog"
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "processor-lifecycle-probe"
            scaffold_standard_processor(
                manifest_path=fixture / "processor.yaml",
                calculation_path=fixture / "calculation.py",
                cases_path=fixture / "cases.yaml",
                input_catalog_path=catalog / "input-catalog.yaml",
                approval_catalog_path=catalog / "derived-output-approvals.yaml",
                output_directory=output,
            )

            self.assertIn(
                '"processor_lifecycle_probe.main"',
                (output / "Dockerfile").read_text(encoding="utf-8"),
            )

    def test_locks_generated_files_without_locking_author_owned_directories(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "generated.txt").write_text("generated\n", encoding="utf-8")
            (root / "docs").mkdir()
            (root / "docs" / "notes.md").write_text("author\n", encoding="utf-8")

            lock_generated_files(root, ("docs",))
            (root / "docs" / "notes.md").write_text("changed\n", encoding="utf-8")

            self.assertTrue(verify_generated_files(root)["passed"])
            (root / "generated.txt").write_text("changed\n", encoding="utf-8")
            with self.assertRaisesRegex(AuthoringValidationError, "generated file was modified"):
                verify_generated_files(root)

    def test_renders_shell_expansions_without_forgejo_expression_syntax(self) -> None:
        workflow = _workflow_text(parse_manifest(formula_manifest_document()))

        self.assertIn('registry="${FORGEJO_SERVER_URL#https://}"', workflow)
        self.assertIn('${{ steps.registry.outputs.image }}', workflow)
        self.assertNotIn('${{ FORGEJO_SERVER_URL#https:// }}', workflow)
        self.assertIsInstance(yaml.safe_load(workflow), dict)

    @staticmethod
    def _input_catalog(document: dict[str, object] | None = None):
        from wama_processor_authoring.catalog import InputCatalog, CatalogSignal

        document = input_catalog_document() if document is None else document
        signals = {
            signal["reference"]: CatalogSignal(
                reference=signal["reference"],
                mrid=signal["mrid"],
                value_kind=signal["value_kind"],
                quantity=signal["quantity"],
                unit=signal["unit"],
            )
            for signal in document["signals"]
        }
        return InputCatalog(
            catalog_id=document["catalog_id"],
            revision=document["revision"],
            signals=signals,
        )

    @staticmethod
    def _approval_catalog(document: dict[str, object] | None = None):
        from wama_processor_authoring.catalog import (
            ApprovalCatalog,
            ApprovedOutput,
            ApprovedTypedContract,
        )

        document = approval_catalog_document() if document is None else document
        outputs = {
            output["mrid"]: ApprovedOutput(
                mrid=output["mrid"],
                value_kind=output["value"],
                unit=output["unit"],
                topic=output["topic"],
                owner=output["owner"],
                approval=output["approval"],
            )
            for output in document["outputs"]
        }
        typed_contracts = {
            contract["id"]: ApprovedTypedContract(
                contract_id=contract["id"],
                topic=contract["topic"],
                protobuf_type=contract["protobuf_type"],
                owner=contract["owner"],
                approval=contract["approval"],
            )
            for contract in document.get("typed_contracts", [])
        }
        return ApprovalCatalog(
            catalog_id=document["catalog_id"],
            revision=document["revision"],
            outputs=outputs,
            typed_contracts=typed_contracts,
        )

    def _latest_values_processor(self):
        manifest_document = latest_values_manifest_document()
        input_document = input_catalog_document()
        input_document["signals"].extend(
            [
                {
                    "reference": "pmu-bay-01.voltage-l1",
                    "mrid": "urn:wama:poc:pmu:bay-01:voltage-l1",
                    "value_kind": "double",
                    "quantity": "voltage",
                    "unit": "V",
                },
                {
                    "reference": "pmu-bay-01.current-l1",
                    "mrid": "urn:wama:poc:pmu:bay-01:current-l1",
                    "value_kind": "double",
                    "quantity": "current",
                    "unit": "A",
                },
            ]
        )
        input_document["revision"] = content_revision(input_document)
        approvals = self._approval_catalog()
        return resolve_processor(
            parse_manifest(manifest_document),
            self._input_catalog(input_document),
            approvals,
        )