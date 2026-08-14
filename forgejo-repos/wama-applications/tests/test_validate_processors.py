"""Tests for application-only processor validation."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from scripts.validate_processors import validate_structure


class ValidateProcessorsTests(unittest.TestCase):
    """Validate application processor contracts without reading infra files."""

    def test_accepts_complete_application_processor(self) -> None:
        with TemporaryDirectory() as directory:
            root = self._create_application_root(Path(directory), include_processor=True)

            self.assertEqual(validate_structure(root), [])

    def test_rejects_cross_project_dependency(self) -> None:
        with TemporaryDirectory() as directory:
            root = self._create_application_root(Path(directory), include_processor=True)
            fragment = root / "processors" / "processor-frequency" / "compose.yaml"
            fragment.write_text(
                fragment.read_text(encoding="utf-8")
                + "    depends_on:\n      kafka:\n        condition: service_healthy\n",
                encoding="utf-8",
            )

            self.assertIn(
                "processor-frequency must not use cross-project depends_on",
                validate_structure(root),
            )

    def test_rejects_processor_missing_from_application_include(self) -> None:
        with TemporaryDirectory() as directory:
            root = self._create_application_root(Path(directory), include_processor=False)

            self.assertIn(
                "processor-frequency is missing from application compose includes",
                validate_structure(root),
            )

    def _create_application_root(self, root: Path, include_processor: bool) -> Path:
        processor = root / "processors" / "processor-frequency"
        package = processor / "src" / "processor_frequency"
        package.mkdir(parents=True)
        (processor / "tests").mkdir()
        (processor / "README.md").write_text("# Processor\n", encoding="utf-8")
        (processor / "requirements.txt").write_text("quixstreams\n", encoding="utf-8")
        (processor / "processor.yaml").write_text(
            "consumer_group: processor-frequency\n",
            encoding="utf-8",
        )
        (processor / "Dockerfile").write_text(
            "COPY contracts/rtd_schema.proto /build/proto/rtd_schema.proto\n"
            "COPY processors/processor-frequency/src /build/src\n",
            encoding="utf-8",
        )
        (processor / "compose.yaml").write_text(
            "services:\n"
            "  processor-frequency:\n"
            "    image: \"${WAMA_APPLICATION_IMAGE_PREFIX:-wama.local/wama-applications}/processor-frequency:${WAMA_APPLICATION_IMAGE_TAG:-main}\"\n"
            "    networks:\n"
            "      - wama-infra\n"
            "networks:\n"
            "  wama-infra:\n"
            "    external: true\n",
            encoding="utf-8",
        )
        (root / "compose.yaml").write_text(
            "services: {}\n"
            "networks:\n"
            "  wama-infra:\n"
            "    external: true\n"
            "include:\n"
            + (
                "  - ./processors/processor-frequency/compose.yaml\n"
                if include_processor
                else ""
            ),
            encoding="utf-8",
        )
        return root