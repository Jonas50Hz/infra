"""Tests for app-local processor provisioning."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from scripts.provision_processor import ProvisioningError, provision_processor


class ProvisionProcessorTests(unittest.TestCase):
    """Ensure app provisioning never updates parent infrastructure files."""

    def test_provisions_only_application_files(self) -> None:
        with TemporaryDirectory() as directory:
            application_root = self._create_application_root(Path(directory))

            service_name = provision_processor(application_root, "frequency-scale")

            self.assertEqual(service_name, "processor-frequency-scale")
            self.assertTrue(
                (
                    application_root
                    / "processors"
                    / service_name
                    / "src"
                    / "processor_frequency_scale"
                ).is_dir()
            )
            self.assertIn(
                "./processors/processor-frequency-scale/compose.yaml",
                (application_root / "compose.yaml").read_text(encoding="utf-8"),
            )
            self.assertIn(
                "PROCESSOR_FREQUENCY_SCALE_CONFIG_SOURCE",
                (
                    application_root / "processors" / service_name / "compose.yaml"
                ).read_text(encoding="utf-8"),
            )

    def test_rejects_invalid_processor_name(self) -> None:
        with TemporaryDirectory() as directory:
            application_root = self._create_application_root(Path(directory))

            with self.assertRaises(ProvisioningError):
                provision_processor(application_root, "Frequency Scale")

    def _create_application_root(self, root: Path) -> Path:
        template = root / "templates" / "quixstreams-processor"
        package = template / "src" / "processor_template"
        package.mkdir(parents=True)
        (template / "Dockerfile").write_text(
            "COPY processors/processor-template/src /build/src\n",
            encoding="utf-8",
        )
        (template / "compose.yaml").write_text(
            "services:\n"
            "  processor-template:\n"
            "    source: ${PROCESSOR_TEMPLATE_CONFIG_SOURCE:-./processor.yaml}\n"
            "    networks:\n"
            "      - wama-infra\n"
            "networks:\n"
            "  wama-infra:\n"
            "    external: true\n",
            encoding="utf-8",
        )
        (template / "processor.yaml").write_text(
            "consumer_group: processor-template\n",
            encoding="utf-8",
        )
        (package / "main.py").write_text("import processor_template\n", encoding="utf-8")
        (root / "processors").mkdir(exist_ok=True)
        (root / "compose.yaml").write_text(
            "include:\n"
            "  # provisioned-processor-includes:start\n"
            "  # provisioned-processor-includes:end\n",
            encoding="utf-8",
        )
        (root / "README.md").write_text(
            "<!-- provisioned-processors:start -->\n"
            "<!-- provisioned-processors:end -->\n",
            encoding="utf-8",
        )
        return root