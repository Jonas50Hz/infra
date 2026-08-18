"""Tests for Druid supervisor specification and control-plane validation."""

from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock

from druid_init.supervisor import (
    SupervisorError,
    load_supervisor_spec,
    submit_supervisor,
    validate_supervisor_health,
    validate_supervisor_spec,
    validate_supervisor_status,
)


class SupervisorSpecificationTests(unittest.TestCase):
    """Require the immutable raw-Protobuf and no-rollup ingestion contract."""

    def test_accepts_live_measurement_specification(self) -> None:
        specification = _live_measurement_specification()

        validate_supervisor_spec(specification, "live_measurements")

    def test_rejects_rollup(self) -> None:
        specification = _live_measurement_specification()
        specification["spec"]["dataSchema"]["granularitySpec"]["rollup"] = True

        with self.assertRaisesRegex(SupervisorError, "rollup"):
            validate_supervisor_spec(specification, "live_measurements")

    def test_rejects_noncanonical_descriptor(self) -> None:
        specification = _live_measurement_specification()
        specification["spec"]["ioConfig"]["inputFormat"]["valueFormat"]["protoBytesDecoder"][
            "descriptor"
        ] = "file:///tmp/other.desc"

        with self.assertRaisesRegex(SupervisorError, "canonical"):
            validate_supervisor_spec(specification, "live_measurements")

    def test_loads_json_specification(self) -> None:
        specification = _live_measurement_specification()
        with TemporaryDirectory() as directory:
            path = Path(directory) / "supervisor.json"
            path.write_text(json.dumps(specification), encoding="utf-8")

            loaded = load_supervisor_spec(path, "live_measurements")

        self.assertEqual(loaded, specification)


class SupervisorControlPlaneTests(unittest.TestCase):
    """Require idempotent supervisor submission and healthy task status."""

    def test_submits_without_restarting_an_unchanged_supervisor(self) -> None:
        session = Mock()
        response = Mock()
        session.post.return_value = response
        specification = _live_measurement_specification()

        submit_supervisor(session, "http://druid:8888", specification)

        session.post.assert_called_once_with(
            "http://druid:8888/druid/indexer/v1/supervisor",
            params={"skipRestartIfUnmodified": "true"},
            json=specification,
            timeout=10,
        )
        response.raise_for_status.assert_called_once_with()

    def test_accepts_boolean_and_object_health_responses(self) -> None:
        validate_supervisor_health(True)
        validate_supervisor_health({"healthy": True})

    def test_rejects_unhealthy_response(self) -> None:
        with self.assertRaisesRegex(SupervisorError, "health"):
            validate_supervisor_health({"healthy": False})

    def test_accepts_running_status_without_errors(self) -> None:
        validate_supervisor_status(
            {
                "id": "live_measurements",
                "detailedState": "RUNNING",
                "recentErrors": [],
            },
            "live_measurements",
        )

    def test_accepts_druid_status_payload_envelope(self) -> None:
        validate_supervisor_status(
            {
                "id": "live_measurements",
                "payload": {
                    "id": "live_measurements",
                    "detailedState": "RUNNING",
                    "recentErrors": [],
                },
            },
            "live_measurements",
        )

    def test_rejects_saved_parse_errors(self) -> None:
        with self.assertRaisesRegex(SupervisorError, "errors"):
            validate_supervisor_status(
                {
                    "id": "live_measurements",
                    "detailedState": "RUNNING",
                    "recentErrors": ["parse error"],
                },
                "live_measurements",
            )


def _live_measurement_specification() -> dict[str, object]:
    path = Path("/etc/wama/supervisors/live-measurements.json")
    return json.loads(path.read_text(encoding="utf-8"))