"""Tests for pure infrastructure readiness validation logic."""

from __future__ import annotations

from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock, patch

from infra_readiness.checks import (
    REQUIRED_TOPICS,
    ReadinessError,
    TOPIC_CLEANUP_POLICIES,
    check_postgres,
    topic_configurations_from_responses,
    validate_grafana_dashboard,
    validate_grafana_datasource,
    validate_grafana_pmu_dashboard,
    validate_grafana_pmu_query,
    validate_forgejo_repository,
    validate_forgejo_runners,
    validate_iec104_browser_status,
    validate_live_measurement,
    validate_prometheus_up,
    validate_topic_configurations,
    validate_topic_descriptions,
)
from infra_readiness.druid import (
    DruidReadinessError,
    validate_measurement_rows,
    validate_router_health,
    validate_supervisor_health,
    validate_supervisor_status,
)
from infra_readiness.generated import rtd_schema_pb2


class KafkaContractTests(unittest.TestCase):
    """Validate the broker and raw Common Format evidence checks."""

    def test_accepts_expected_topic_layout_and_cleanup_policies(self) -> None:
        descriptions = [
            {
                "topic": topic,
                "error_code": 0,
                "partitions": [{"replicas": [1], "isr": [1]}],
            }
            for topic in REQUIRED_TOPICS
        ]
        configurations = {
            topic: {"cleanup.policy": cleanup_policy}
            for topic, cleanup_policy in TOPIC_CLEANUP_POLICIES.items()
        }

        validate_topic_descriptions(descriptions)
        validate_topic_configurations(configurations)

    def test_rejects_missing_compacted_cleanup_policy(self) -> None:
        configurations = {
            topic: {"cleanup.policy": "delete"}
            for topic in REQUIRED_TOPICS
        }

        with self.assertRaisesRegex(ReadinessError, "Masterdata"):
            validate_topic_configurations(configurations)

    def test_parses_kafka_python_tuple_config_response(self) -> None:
        response = SimpleNamespace(
            resources=[
                (
                    0,
                    "",
                    2,
                    "LiveMeasurement",
                    [("cleanup.policy", "delete", False, 1, False, [])],
                )
            ]
        )

        configurations = topic_configurations_from_responses([response])

        self.assertEqual(configurations, {"LiveMeasurement": {"cleanup.policy": "delete"}})

    def test_accepts_matching_raw_protobuf_measurement(self) -> None:
        message = rtd_schema_pb2.MCCSMeasurementValue()
        message.mrid = "urn:wama:poc:pmu:bay-01:frequency"
        message.double_value = 50.01
        message.quality.valid = True
        message.timestamp_field.seconds = 1
        message.timestamp_gateway.seconds = 1
        message.timestamp_gateway.nanos = 10_000_000
        message.timestamp_mccs.seconds = 1
        message.timestamp_mccs.nanos = 20_000_000
        record = SimpleNamespace(
            key=message.mrid.encode("utf-8"),
            value=message.SerializeToString(),
            timestamp=1_020,
        )

        validate_live_measurement(record, "urn:wama:poc:pmu:")

    def test_rejects_measurement_with_mismatched_kafka_key(self) -> None:
        message = rtd_schema_pb2.MCCSMeasurementValue()
        message.mrid = "urn:wama:poc:pmu:bay-01:frequency"
        message.double_value = 50.01
        message.quality.valid = True
        message.timestamp_field.seconds = 1
        message.timestamp_gateway.seconds = 1
        message.timestamp_mccs.seconds = 1
        record = SimpleNamespace(
            key=b"unexpected-key",
            value=message.SerializeToString(),
            timestamp=1_000,
        )

        with self.assertRaisesRegex(ReadinessError, "Kafka key"):
            validate_live_measurement(record, "urn:wama:poc:pmu:")


class ControlPlaneTests(unittest.TestCase):
    """Validate the REST and metrics response expectations."""

    def test_accepts_druid_router_and_supervisor_health(self) -> None:
        validate_router_health(True)
        validate_supervisor_health({"healthy": True})
        validate_supervisor_status(
            {
                "id": "live_measurements",
                "detailedState": "RUNNING",
                "recentErrors": [],
            },
            "live_measurements",
        )

    def test_accepts_provisioned_grafana_druid_measurement_views(self) -> None:
        validate_grafana_datasource(
            {
                "uid": "druid",
                "type": "grafadruid-druid-datasource",
            },
            "druid",
            "grafadruid-druid-datasource",
            "Druid",
        )
        validate_grafana_dashboard(
            {
                "meta": {
                    "provisioned": True,
                    "folderTitle": "WAMA Measurements",
                },
                "dashboard": {
                    "uid": "wama-pmu-live-measurements",
                },
            },
            "wama-pmu-live-measurements",
            "WAMA Measurements",
            "PMU Live Measurements",
        )

    def test_rejects_grafana_druid_dashboard_in_wrong_folder(self) -> None:
        with self.assertRaisesRegex(ReadinessError, "PMU Live Measurements"):
            validate_grafana_dashboard(
                {
                    "meta": {
                        "provisioned": True,
                        "folderTitle": "WAMA Infrastructure",
                    },
                    "dashboard": {
                        "uid": "wama-pmu-live-measurements",
                    },
                },
                "wama-pmu-live-measurements",
                "WAMA Measurements",
                "PMU Live Measurements",
            )

    def test_accepts_unit_safe_grafana_pmu_dashboard(self) -> None:
        validate_grafana_pmu_dashboard(
            {"dashboard": {"panels": self._pmu_dashboard_panels()}}
        )

    def test_rejects_grafana_pmu_dashboard_with_wrong_rocof_unit(self) -> None:
        panels = self._pmu_dashboard_panels()
        panels[3]["fieldConfig"]["defaults"]["unit"] = "hertz"
        with self.assertRaisesRegex(ReadinessError, "ROCOF"):
            validate_grafana_pmu_dashboard({"dashboard": {"panels": panels}})

    @staticmethod
    def _pmu_dashboard_panels() -> list[dict[str, object]]:
        panels: list[dict[str, object]] = []
        for title, panel_type, unit in (
            ("Phase Voltages", "timeseries", "volt"),
            ("Phase Currents", "timeseries", "amp"),
            ("Frequency", "timeseries", "hertz"),
            ("ROCOF (Hz/s)", "timeseries", "suffix:Hz/s"),
            ("Latest Valid PMU Records", "table", None),
        ):
            defaults = {} if unit is None else {"unit": unit}
            panels.append(
                {
                    "title": title,
                    "type": panel_type,
                    "datasource": {
                        "uid": "druid",
                        "type": "grafadruid-druid-datasource",
                    },
                    "fieldConfig": {"defaults": defaults},
                }
            )
        return panels

    def test_accepts_grafana_druid_pmu_query_frame(self) -> None:
        validate_grafana_pmu_query(
            {
                "results": {
                    "A": {
                        "status": 200,
                        "frames": [
                            {
                                "schema": {
                                    "fields": [
                                        {"name": "__time"},
                                        {"name": "mrid"},
                                        {"name": "double_value"},
                                    ]
                                },
                                "data": {
                                    "values": [
                                        [1_787_054_665_443],
                                        ["urn:wama:poc:pmu:bay-01:frequency"],
                                        [50.005],
                                    ]
                                },
                            }
                        ],
                    }
                }
            },
            "urn:wama:poc:pmu:bay-01:frequency",
            50.01,
            0.01,
        )

    def test_rejects_grafana_druid_pmu_query_with_wrong_value(self) -> None:
        with self.assertRaisesRegex(ReadinessError, "double_value"):
            validate_grafana_pmu_query(
                {
                    "results": {
                        "A": {
                            "status": 200,
                            "frames": [
                                {
                                    "schema": {
                                        "fields": [
                                            {"name": "__time"},
                                            {"name": "mrid"},
                                            {"name": "double_value"},
                                        ]
                                    },
                                    "data": {
                                        "values": [
                                            [1_787_054_665_443],
                                            ["urn:wama:poc:pmu:bay-01:frequency"],
                                            [49.99],
                                        ]
                                    },
                                }
                            ],
                        }
                    }
                },
                "urn:wama:poc:pmu:bay-01:frequency",
                50.01,
                0.01,
            )

    def test_accepts_druid_supervisor_status_payload_envelope(self) -> None:
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

    def test_accepts_queryable_druid_pmu_frequency_row(self) -> None:
        validate_measurement_rows(
            [
                {
                    "__time": "2026-08-18T12:00:00.000Z",
                    "mrid": "urn:wama:poc:pmu:bay-01:frequency",
                    "double_value": 50.005,
                    "quality_valid": "true",
                    "timestamp_mccs": "2026-08-18T12:00:00.000Z",
                }
            ],
            "urn:wama:poc:pmu:bay-01:frequency",
            50.01,
            0.01,
        )

    def test_rejects_queryable_druid_pmu_frequency_outside_tolerance(self) -> None:
        with self.assertRaisesRegex(DruidReadinessError, "expected PMU range"):
            validate_measurement_rows(
                [
                    {
                        "__time": "2026-08-18T12:00:00.000Z",
                        "mrid": "urn:wama:poc:pmu:bay-01:frequency",
                        "double_value": 50.03,
                        "quality_valid": "true",
                        "timestamp_mccs": "2026-08-18T12:00:00.000Z",
                    }
                ],
                "urn:wama:poc:pmu:bay-01:frequency",
                50.01,
                0.01,
            )

    def test_rejects_druid_row_with_mismatched_mccs_timestamp(self) -> None:
        with self.assertRaisesRegex(DruidReadinessError, "timestamp_mccs"):
            validate_measurement_rows(
                [
                    {
                        "__time": "2026-08-18T12:00:00.000Z",
                        "mrid": "urn:wama:poc:pmu:bay-01:frequency",
                        "double_value": 50.01,
                        "quality_valid": True,
                        "timestamp_mccs": "2026-08-18T12:00:01.000Z",
                    }
                ],
                "urn:wama:poc:pmu:bay-01:frequency",
                50.01,
            )

    def test_rejects_druid_supervisor_parse_errors(self) -> None:
        with self.assertRaisesRegex(DruidReadinessError, "errors"):
            validate_supervisor_status(
                {
                    "id": "live_measurements",
                    "detailedState": "RUNNING",
                    "recentErrors": ["parse error"],
                },
                "live_measurements",
            )

    def test_accepts_private_seeded_processor_repository(self) -> None:
        validate_forgejo_repository(
            {
                "private": True,
                "empty": False,
                "default_branch": "main",
            },
            "processor-frequency-scale",
        )

    def test_rejects_unseeded_processor_repository(self) -> None:
        with self.assertRaisesRegex(ReadinessError, "private and seeded"):
            validate_forgejo_repository(
                {
                    "private": True,
                    "empty": True,
                    "default_branch": "main",
                },
                "processor-apparent-power",
            )

    def test_accepts_registered_idle_runner_connections(self) -> None:
        validate_forgejo_runners(
            "processor-frequency-scale",
            [
                {
                    "name": "wama-processor-frequency-scale-ci",
                    "status": "idle",
                    "labels": ["wama-processors-ci:docker://wama-forgejo-runner:local"],
                },
                {
                    "name": "wama-processor-frequency-scale-deploy",
                    "status": "idle",
                    "labels": ["wama-processors-deploy:host"],
                },
            ],
        )

    def test_rejects_missing_deployment_runner_connection(self) -> None:
        with self.assertRaisesRegex(
            ReadinessError,
            "wama-processor-apparent-power-deploy",
        ):
            validate_forgejo_runners(
                "processor-apparent-power",
                [
                    {
                        "name": "wama-processor-apparent-power-ci",
                        "status": "idle",
                        "labels": ["wama-processors-ci"],
                    }
                ],
            )

    def test_accepts_up_metrics(self) -> None:
        validate_prometheus_up(
            {
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [{"value": ["0", "1"]}],
                },
            },
            "kafka-exporter",
        )

    def test_rejects_down_metrics(self) -> None:
        with self.assertRaisesRegex(ReadinessError, "down"):
            validate_prometheus_up(
                {
                    "status": "success",
                    "data": {
                        "resultType": "vector",
                        "result": [{"value": ["0", "0"]}],
                    },
                },
                "kafka-exporter",
            )

    def test_accepts_idle_or_active_iec104_browser_status(self) -> None:
        validate_iec104_browser_status({"active": False, "state": "idle", "viewers": 0})
        validate_iec104_browser_status({"active": True, "state": "active", "viewers": 1})

    def test_rejects_inconsistent_iec104_browser_status(self) -> None:
        with self.assertRaisesRegex(ReadinessError, "active state"):
            validate_iec104_browser_status({"active": False, "state": "active", "viewers": 1})
        with self.assertRaisesRegex(ReadinessError, "viewer count"):
            validate_iec104_browser_status({"active": False, "state": "idle", "viewers": -1})


class PostgreSQLReadinessTests(unittest.TestCase):
    """Keep the infrastructure gate independent from app-owned catalog tables."""

    def test_accepts_expected_identity_without_querying_application_tables(self) -> None:
        connection = MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = ("wama", "wama")
        settings = SimpleNamespace(
            postgres_dsn="postgresql://wama:wama-postgres-password@postgres:5432/wama",
            postgres_database="wama",
            postgres_user="wama",
        )

        with patch("infra_readiness.checks.psycopg.connect", return_value=connection):
            check_postgres(settings)

        cursor.execute.assert_called_once_with("SELECT current_database(), current_user;")