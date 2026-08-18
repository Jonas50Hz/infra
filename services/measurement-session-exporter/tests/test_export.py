"""Tests for idempotent immutable object upload and raw-Protobuf publication."""

from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from botocore.exceptions import ClientError

from measurement_session_common.contract import ContractValidationError, validate_measurement_session
from measurement_session_common.generated.measurement_session_pb2 import MeasurementSession
from measurement_session_exporter.config import ArtifactFixture, FinalizedSessionFixture
from measurement_session_exporter.export import export_finalized_session


class _Future:
    def get(self, timeout: int) -> None:
        del timeout


class _Producer:
    def __init__(self) -> None:
        self.records: list[dict[str, object]] = []

    def send(self, topic: str, **kwargs: object) -> _Future:
        self.records.append({"topic": topic, **kwargs})
        return _Future()


class _S3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], dict[str, object]] = {}

    def head_object(self, Bucket: str, Key: str) -> dict[str, object]:
        try:
            object_data = self.objects[(Bucket, Key)]
        except KeyError as error:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject") from error
        return {
            "ContentLength": len(object_data["body"]),
            "ContentType": object_data["content_type"],
            "Metadata": object_data["metadata"],
        }

    def put_object(self, Bucket: str, Key: str, Body: object, ContentType: str, Metadata: dict[str, str]) -> None:
        payload = Body.read() if hasattr(Body, "read") else Body
        assert isinstance(payload, bytes)
        self.objects[(Bucket, Key)] = {
            "body": payload,
            "content_type": ContentType,
            "metadata": Metadata,
        }

    def get_object(self, Bucket: str, Key: str) -> dict[str, BytesIO]:
        return {"Body": BytesIO(self.objects[(Bucket, Key)]["body"])}


class FinalizedSessionExportTests(unittest.TestCase):
    """Prove a fixture publishes a key-aligned raw-Protobuf record."""

    def test_exports_immutable_objects_then_publishes_raw_protobuf(self) -> None:
        with TemporaryDirectory() as directory:
            artifact_path = Path(directory) / "waveform.csv"
            artifact_path.write_text(_complete_waveform(), encoding="utf-8")
            fixture = FinalizedSessionFixture(
                session_id="4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237",
                source_mrid="urn:wama:poc:pmu:bay-01",
                started_at=datetime(2026, 8, 18, 9, 0, tzinfo=timezone.utc),
                ended_at=datetime(2026, 8, 18, 9, 1, tzinfo=timezone.utc),
                finalized_at=datetime(2026, 8, 18, 9, 2, tzinfo=timezone.utc),
                measurement_count=3,
                metadata=(("asset", "bay-01"),),
                artifacts=(ArtifactFixture("waveform", "text/csv", artifact_path),),
            )
            storage = _S3()
            producer = _Producer()

            result = export_finalized_session(fixture, storage, producer, "MeasurementSession")
            replay = export_finalized_session(fixture, storage, producer, "MeasurementSession")

        self.assertEqual(result.payload, replay.payload)
        self.assertEqual(len(storage.objects), 2)
        self.assertEqual(len(producer.records), 2)
        record = producer.records[0]
        self.assertEqual(record["topic"], "MeasurementSession")
        self.assertEqual(record["key"], fixture.session_id.encode("utf-8"))
        decoded = MeasurementSession()
        decoded.ParseFromString(record["value"])
        validate_measurement_session(decoded)

    def test_rejects_boundary_only_waveform_before_uploading_any_object(self) -> None:
        with TemporaryDirectory() as directory:
            artifact_path = Path(directory) / "waveform.csv"
            artifact_path.write_text(
                "timestamp,voltage_l1\n"
                "2026-08-18T09:00:00Z,230.1\n"
                "2026-08-18T09:01:00Z,230.0\n",
                encoding="utf-8",
            )
            fixture = FinalizedSessionFixture(
                session_id="4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237",
                source_mrid="urn:wama:poc:pmu:bay-01",
                started_at=datetime(2026, 8, 18, 9, 0, tzinfo=timezone.utc),
                ended_at=datetime(2026, 8, 18, 9, 1, tzinfo=timezone.utc),
                finalized_at=datetime(2026, 8, 18, 9, 2, tzinfo=timezone.utc),
                measurement_count=3,
                metadata=(),
                artifacts=(ArtifactFixture("waveform", "text/csv", artifact_path),),
            )
            storage = _S3()

            with self.assertRaisesRegex(ContractValidationError, "does not contain every"):
                export_finalized_session(fixture, storage, _Producer(), "MeasurementSession")

        self.assertEqual(storage.objects, {})

    def test_rejects_invalid_artifact_before_uploading_any_object(self) -> None:
        with TemporaryDirectory() as directory:
            artifact_path = Path(directory) / "waveform.csv"
            artifact_path.write_text("sample\n", encoding="utf-8")
            fixture = FinalizedSessionFixture(
                session_id="4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237",
                source_mrid="urn:wama:poc:pmu:bay-01",
                started_at=datetime(2026, 8, 18, 9, 0, tzinfo=timezone.utc),
                ended_at=datetime(2026, 8, 18, 9, 1, tzinfo=timezone.utc),
                finalized_at=datetime(2026, 8, 18, 9, 2, tzinfo=timezone.utc),
                measurement_count=12,
                metadata=(),
                artifacts=(ArtifactFixture("invalid_artifact", "text/csv", artifact_path),),
            )
            storage = _S3()

            with self.assertRaisesRegex(ValueError, "artifact ids"):
                export_finalized_session(fixture, storage, _Producer(), "MeasurementSession")

        self.assertEqual(storage.objects, {})


def _complete_waveform() -> str:
    return (
        "timestamp,voltage_l1,voltage_l2,voltage_l3\n"
        "2026-08-18T09:00:00Z,230.1,229.8,230.4\n"
        "2026-08-18T09:00:30Z,230.05,229.85,230.3\n"
        "2026-08-18T09:01:00Z,230.0,229.9,230.2\n"
    )