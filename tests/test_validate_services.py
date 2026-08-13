"""Tests for processor structural validation."""

from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from scripts.validate_services import validate_structure


class ValidateServicesTests(unittest.TestCase):
    """Keep validation independent from a local Docker daemon."""

    def test_accepts_complete_provisioned_processor(self) -> None:
        with TemporaryDirectory() as directory:
            root = self._create_repository(Path(directory), include_processor=True)

            self.assertEqual(validate_structure(root), [])

    def test_rejects_processor_missing_from_root_include(self) -> None:
        with TemporaryDirectory() as directory:
            root = self._create_repository(Path(directory), include_processor=False)

            self.assertIn(
                "processor-frequency is missing from docker-compose.yml includes",
                validate_structure(root),
            )

    def test_rejects_an_active_template_service(self) -> None:
        with TemporaryDirectory() as directory:
            root = self._create_repository(Path(directory), include_processor=True)
            (root / "docker-compose.yml").write_text(
                "include:\n"
                "  - ./services/processor-frequency/compose.yaml\n"
                "  - ./services/processor-template/compose.yaml\n",
                encoding="utf-8",
            )

            self.assertIn(
                "processor-template must not be included in docker-compose.yml",
                validate_structure(root),
            )

    def _create_repository(self, root: Path, include_processor: bool) -> Path:
        pmu_gateway = root / "services" / "pmu-gateway"
        pmu_gateway.mkdir(parents=True)
        (pmu_gateway / "compose.yaml").write_text(
            "services:\n"
            "  pmu-gateway:\n"
            "    image: \"${WAMA_IMAGE_PREFIX:-wama.local/wama}/pmu-gateway:${WAMA_IMAGE_TAG:-main}\"\n",
            encoding="utf-8",
        )
        service = root / "services" / "processor-frequency"
        package = service / "src" / "processor_frequency"
        package.mkdir(parents=True)
        (service / "tests").mkdir()
        (service / "README.md").write_text("# Processor\n", encoding="utf-8")
        (service / "requirements.txt").write_text("quixstreams\n", encoding="utf-8")
        (service / "processor.yaml").write_text(
            "consumer_group: processor-frequency\n",
            encoding="utf-8",
        )
        (service / "Dockerfile").write_text(
            "COPY services/processor-frequency/src /build/src\n",
            encoding="utf-8",
        )
        (service / "compose.yaml").write_text(
            "services:\n"
            "  processor-frequency:\n"
            "    image: \"${WAMA_IMAGE_PREFIX:-wama.local/wama}/processor-frequency:${WAMA_IMAGE_TAG:-main}\"\n"
            "    depends_on:\n"
            "      kafka-init:\n"
            "        condition: service_completed_successfully\n",
            encoding="utf-8",
        )
        (root / "docker-compose.yml").write_text(
            "include:\n"
            "  - ./services/pmu-gateway/compose.yaml\n"
            + (
                "  - ./services/processor-frequency/compose.yaml\n"
                if include_processor
                else ""
            ),
            encoding="utf-8",
        )
        return root