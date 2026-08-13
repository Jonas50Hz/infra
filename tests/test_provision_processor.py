"""Tests for processor-service generation and registration."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from scripts.provision_processor import ProvisioningError, provision_processor


class ProvisionProcessorTests(unittest.TestCase):
    """Exercise provisioner behavior without changing the actual workspace."""

    def test_provisions_service_and_updates_registries(self) -> None:
        with TemporaryDirectory() as directory:
            root = self._create_repository(Path(directory))

            service_name = provision_processor(root, "frequency-scale")

            self.assertEqual(service_name, "processor-frequency-scale")
            self.assertTrue(
                (root / "services" / service_name / "src" / "processor_frequency_scale").is_dir()
            )
            self.assertIn(
                "./services/processor-frequency-scale/compose.yaml",
                (root / "docker-compose.yml").read_text(encoding="utf-8"),
            )
            self.assertIn(
                "processor-frequency-scale",
                (root / "README.md").read_text(encoding="utf-8"),
            )
            self.assertIn(
                "PROCESSOR_FREQUENCY_SCALE_CONFIG_SOURCE",
                (root / "services" / service_name / "compose.yaml").read_text(
                    encoding="utf-8"
                ),
            )

    def test_rejects_invalid_service_name(self) -> None:
        with TemporaryDirectory() as directory:
            root = self._create_repository(Path(directory))

            with self.assertRaises(ProvisioningError):
                provision_processor(root, "Frequency Scale")

    def _create_repository(self, root: Path) -> Path:
        template = root / "templates" / "quixstreams-processor"
        package = template / "src" / "processor_template"
        package.mkdir(parents=True)
        (template / "Dockerfile").write_text(
            "COPY templates/quixstreams-processor/src /build/src\n",
            encoding="utf-8",
        )
        (template / "compose.yaml").write_text(
            "services:\n"
            "  processor-template:\n"
            "    source: ${PROCESSOR_TEMPLATE_CONFIG_SOURCE:-./processor.yaml}\n",
            encoding="utf-8",
        )
        (template / "processor.yaml").write_text(
            "consumer_group: processor-template\n",
            encoding="utf-8",
        )
        (package / "main.py").write_text("import processor_template\n", encoding="utf-8")
        (root / "services").mkdir(exist_ok=True)
        (root / "docker-compose.yml").write_text(
            "include:\n  - ./services/pmu-gateway/compose.yaml\n",
            encoding="utf-8",
        )
        (root / "README.md").write_text(
            "<!-- provisioned-processor-services:start -->\n"
            "<!-- provisioned-processor-services:end -->\n",
            encoding="utf-8",
        )
        (root / "services" / "README.md").write_text(
            "<!-- provisioned-processor-services:start -->\n"
            "<!-- provisioned-processor-services:end -->\n",
            encoding="utf-8",
        )
        (root / ".github").mkdir()
        (root / ".github" / "copilot-instructions.md").write_text(
            "<!-- provisioned-processors:start -->\n"
            "<!-- provisioned-processors:end -->\n",
            encoding="utf-8",
        )
        return root