"""Tests for static finalized-session fixture validation."""

from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from measurement_session_exporter.config import ConfigurationError, load_fixture


class FixtureConfigurationTests(unittest.TestCase):
    """Ensure fixtures cannot reach arbitrary host paths or add live state."""

    def test_loads_a_finalized_fixture_and_resolves_its_artifact(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "waveform.csv").write_text("sample\n", encoding="utf-8")
            fixture_path = root / "session.yaml"
            fixture_path.write_text(
                "session:\n"
                "  session_id: 4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237\n"
                "  source_mrid: urn:wama:poc:pmu:bay-01\n"
                "  started_at: '2026-08-18T09:00:00Z'\n"
                "  ended_at: '2026-08-18T09:00:05Z'\n"
                "  finalized_at: '2026-08-18T09:00:06Z'\n"
                "  measurement_count: 1\n"
                "  metadata: []\n"
                "artifacts:\n"
                "  - id: waveform\n"
                "    path: waveform.csv\n"
                "    content_type: text/csv\n",
                encoding="utf-8",
            )

            fixture = load_fixture(fixture_path)

            self.assertEqual(fixture.artifacts[0].path, root / "waveform.csv")

    def test_default_waveform_contains_every_declared_measurement(self) -> None:
        fixture = load_fixture(Path("/app/fixture/finalized-session.yaml"))

        self.assertEqual(len(fixture.artifacts), 1)
        with fixture.artifacts[0].path.open(encoding="utf-8", newline="") as artifact_file:
            rows = list(csv.DictReader(artifact_file))

        self.assertEqual(len(rows), fixture.measurement_count)
        self.assertGreater(len(rows), 2)
        self.assertTrue(
            all(
                row.get(column)
                for row in rows
                for column in ("timestamp", "voltage_l1", "voltage_l2", "voltage_l3")
            )
        )
        timestamps = tuple(
            datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00")).astimezone(timezone.utc)
            for row in rows
        )
        self.assertEqual(timestamps[0], fixture.started_at)
        self.assertEqual(timestamps[-1], fixture.ended_at)
        self.assertEqual(timestamps, tuple(sorted(timestamps)))
        self.assertEqual(len(timestamps), len(set(timestamps)))

    def test_rejects_artifact_outside_the_fixture_directory(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            fixture_path = root / "session.yaml"
            fixture_path.write_text(
                "session:\n"
                "  session_id: 4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237\n"
                "  source_mrid: urn:wama:poc:pmu:bay-01\n"
                "  started_at: '2026-08-18T09:00:00Z'\n"
                "  ended_at: '2026-08-18T09:00:05Z'\n"
                "  finalized_at: '2026-08-18T09:00:06Z'\n"
                "  measurement_count: 1\n"
                "artifacts:\n"
                "  - id: waveform\n"
                "    path: ../outside.csv\n"
                "    content_type: text/csv\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(ConfigurationError, "not a local file"):
                load_fixture(fixture_path)