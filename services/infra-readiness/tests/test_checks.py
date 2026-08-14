"""Tests for pure infrastructure readiness validation logic."""

from __future__ import annotations

from types import SimpleNamespace
import unittest

from infra_readiness.checks import (
    REQUIRED_TOPICS,
    ReadinessError,
    TOPIC_CLEANUP_POLICIES,
    topic_configurations_from_responses,
    validate_forgejo_runners,
    validate_live_measurement,
    validate_prometheus_up,
    validate_topic_configurations,
    validate_topic_descriptions,
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

    def test_accepts_registered_idle_runner_with_required_labels(self) -> None:
        validate_forgejo_runners(
            [
                {
                    "name": "wama-applications",
                    "status": "idle",
                    "labels": [
                        "wama-app-ci:docker://wama-forgejo-runner:local",
                        "wama-app-deploy:host",
                    ],
                }
            ],
            "wama-applications",
        )

    def test_rejects_runner_missing_deployment_label(self) -> None:
        with self.assertRaisesRegex(ReadinessError, "wama-app-deploy"):
            validate_forgejo_runners(
                [{"name": "wama-applications", "status": "idle", "labels": ["wama-app-ci"]}],
                "wama-applications",
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