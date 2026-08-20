"""Kafka offset behavior for one-way IEC 104 delivery."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
import unittest

from google.protobuf.timestamp_pb2 import Timestamp

from iec104_common.generated import iec104_export_pb2
from iec104_exporter.config import Settings
from iec104_exporter.consumer import ExportWorker


class _Transport:
    def __init__(self, accepted: bool) -> None:
        self.accepted = accepted
        self.active_control_center = True
        self.records: list[iec104_export_pb2.ExportRecord] = []

    def publish(self, record: iec104_export_pb2.ExportRecord) -> bool:
        self.records.append(record)
        return self.accepted


class _Consumer:
    def __init__(self, worker: ExportWorker, record: object) -> None:
        self._worker = worker
        self._record = record
        self.commits = 0
        self.commit_offsets: list[dict[object, object]] = []
        self.seeks: list[tuple[object, int]] = []
        self._polled = False

    def poll(self, timeout_ms: int) -> dict[object, list[object]]:
        del timeout_ms
        if self._polled:
            return {}
        self._polled = True
        return {"partition": [self._record]}

    def commit(self, offsets: dict[object, object]) -> None:
        self.commits += 1
        self.commit_offsets.append(offsets)
        self._worker._stop.set()

    def seek(self, partition: object, offset: int) -> None:
        self.seeks.append((partition, offset))
        self._worker._stop.set()

    def close(self, autocommit: bool) -> None:
        del autocommit


class ExportWorkerTests(unittest.TestCase):
    """Commit only after c104 accepts the current Kafka record."""

    def test_commits_after_c104_accepts_the_batch(self) -> None:
        transport = _Transport(accepted=True)
        worker = ExportWorker(_settings(), transport)
        record = _kafka_record()
        consumer = _Consumer(worker, record)
        worker._consumer_factory = lambda *args, **kwargs: consumer

        worker._run()

        self.assertEqual(consumer.commits, 1)
        self.assertEqual(consumer.seeks, [])
        self.assertEqual(len(transport.records), 1)
        offset_metadata = next(iter(consumer.commit_offsets[0].values()))
        self.assertEqual(offset_metadata.offset, record.offset + 1)

    def test_rewinds_without_committing_when_c104_cannot_send(self) -> None:
        transport = _Transport(accepted=False)
        worker = ExportWorker(_settings(), transport)
        record = _kafka_record()
        consumer = _Consumer(worker, record)
        worker._consumer_factory = lambda *args, **kwargs: consumer

        worker._run()

        self.assertEqual(consumer.commits, 0)
        self.assertEqual(len(consumer.seeks), 1)
        self.assertEqual(consumer.seeks[0][1], record.offset)


def _settings() -> Settings:
    return Settings(
        bind_host="0.0.0.0",
        backend_port=2405,
        kafka_bootstrap_servers="kafka:9092",
        kafka_consumer_group="iec104-exporter",
        kafka_topic="Export",
        port=2404,
        ready_file="/tmp/iec104-exporter-ready",
        retry_interval_seconds=0.001,
    )


def _kafka_record() -> SimpleNamespace:
    message = iec104_export_pb2.ExportRecord(
        export_id="4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237",
    )
    timestamp = Timestamp()
    timestamp.FromDatetime(datetime(2026, 8, 19, 12, 0, tzinfo=timezone.utc))
    message.created_at.CopyFrom(timestamp)
    message.iec104_asdu.type_id = iec104_export_pb2.IEC104_TYPE_ID_M_SP_NA_1
    message.iec104_asdu.common_address = 1
    message.iec104_asdu.cause.code = 3
    information_object = message.iec104_asdu.information_objects.add()
    information_object.information_object_address = 1
    information_object.single_point.value = True
    return SimpleNamespace(
        key=message.export_id.encode("utf-8"),
        value=message.SerializeToString(),
        timestamp=timestamp.seconds * 1_000 + timestamp.nanos // 1_000_000,
        topic="Export",
        partition=0,
        offset=7,
    )


if __name__ == "__main__":
    unittest.main()