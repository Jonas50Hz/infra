"""Tests for initial compacted-Masterdata reconciliation."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest

from gateway_dashboard_provisioner.config import Settings
from gateway_dashboard_provisioner.consumer import MasterdataDashboardWorker
from gateway_dashboard_provisioner.generated import masterdata_pb2
from gateway_dashboard_provisioner.render import source_dashboard_filename
from gateway_dashboard_provisioner.storage import DashboardStore
from kafka import TopicPartition


class InitialSnapshotTests(unittest.TestCase):
    """A restart must reconstruct active dashboards from the compacted topic."""

    def test_rebuilds_active_sources_and_applies_tombstones_before_publish(self) -> None:
        source = _source_message("pmu-bay-01")
        partition = TopicPartition("Masterdata", 0)
        consumer = _Consumer(
            partition,
            [
                SimpleNamespace(key=b"pmu-bay-01", value=source.SerializeToString()),
                SimpleNamespace(key=b"pmu-bay-01", value=None),
            ],
        )
        with TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            settings = Settings(
                dashboard_directory=str(directory),
                kafka_bootstrap_servers="kafka:9092",
                kafka_retry_interval_seconds=1,
                kafka_topic="Masterdata",
                ready_file=str(directory / "ready"),
            )
            worker = MasterdataDashboardWorker(settings, DashboardStore(directory))

            registry = worker.reconcile_initial_snapshot(consumer)
            DashboardStore(directory).publish(registry.sources)

            self.assertEqual(registry.sources, ())
            self.assertTrue((directory / "fleet.json").is_file())
            self.assertFalse((directory / source_dashboard_filename("pmu-bay-01")).exists())
            self.assertEqual(consumer.assignments, (partition,))

    def test_rebuilds_source_from_a_compacted_upsert(self) -> None:
        partition = TopicPartition("Masterdata", 0)
        source = _source_message("pmu-bay-01")
        consumer = _Consumer(
            partition,
            [SimpleNamespace(key=b"pmu-bay-01", value=source.SerializeToString())],
        )
        with TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            worker = MasterdataDashboardWorker(
                Settings(str(directory), "kafka:9092", 1, "Masterdata", str(directory / "ready")),
                DashboardStore(directory),
            )

            registry = worker.reconcile_initial_snapshot(consumer)

            self.assertEqual([item.source_id for item in registry.sources], ["pmu-bay-01"])

    def test_tails_a_tombstone_and_removes_the_generated_source_file(self) -> None:
        partition = TopicPartition("Masterdata", 0)
        source = _source_message("pmu-bay-01")
        with TemporaryDirectory() as temporary_directory:
            directory = Path(temporary_directory)
            worker = MasterdataDashboardWorker(
                Settings(str(directory), "kafka:9092", 1, "Masterdata", str(directory / "ready")),
                DashboardStore(directory),
            )
            registry = worker.reconcile_initial_snapshot(
                _Consumer(
                    partition,
                    [SimpleNamespace(key=b"pmu-bay-01", value=source.SerializeToString())],
                )
            )
            store = DashboardStore(directory)
            store.publish(registry.sources)

            worker._tail(
                _TailConsumer(worker, partition, SimpleNamespace(key=b"pmu-bay-01", value=None)),
                registry,
            )

            self.assertEqual(registry.sources, ())
            self.assertFalse((directory / source_dashboard_filename("pmu-bay-01")).exists())


class _Consumer:
    def __init__(self, partition: TopicPartition, records: list[SimpleNamespace]) -> None:
        self._partition = partition
        self._records = records
        self._position = 0
        self.assignments: tuple[TopicPartition, ...] = ()

    def partitions_for_topic(self, topic: str) -> set[int]:
        return {self._partition.partition}

    def assign(self, assignments: tuple[TopicPartition, ...]) -> None:
        self.assignments = assignments

    def seek_to_beginning(self, *assignments: TopicPartition) -> None:
        self._position = 0

    def end_offsets(self, assignments: tuple[TopicPartition, ...]) -> dict[TopicPartition, int]:
        return {self._partition: len(self._records)}

    def position(self, partition: TopicPartition) -> int:
        return self._position

    def poll(self, timeout_ms: int) -> dict[TopicPartition, list[SimpleNamespace]]:
        if self._position:
            return {}
        self._position = len(self._records)
        return {self._partition: self._records}


class _TailConsumer:
    def __init__(
        self,
        worker: MasterdataDashboardWorker,
        partition: TopicPartition,
        record: SimpleNamespace,
    ) -> None:
        self._worker = worker
        self._partition = partition
        self._record = record
        self._polled = False

    def poll(self, timeout_ms: int) -> dict[TopicPartition, list[SimpleNamespace]]:
        if self._polled:
            return {}
        self._polled = True
        self._worker.stop()
        return {self._partition: [self._record]}


def _source_message(source_id: str) -> masterdata_pb2.SourceMasterdata:
    message = masterdata_pb2.SourceMasterdata(
        source_id=source_id,
        catalog_id="wama-c37-118",
        catalog_revision="abc123",
    )
    message.published_at.FromDatetime(datetime(2026, 8, 21, 12, tzinfo=timezone.utc))
    message.location.site_id = "wama-poc-bay-01"
    message.location.display_name = "WAMA PoC Bay 01"
    message.c37_118_tcp.ip_address = "192.0.2.10"
    message.c37_118_tcp.port = 4712
    message.c37_118_tcp.pmu_idcode = 1001
    signal = message.signals.add()
    signal.signal_id = "frequency"
    signal.source_channel = "FREQ"
    signal.mrid = "urn:wama:poc:pmu:bay-01:frequency"
    signal.value_kind = masterdata_pb2.MCCS_VALUE_KIND_DOUBLE
    signal.quantity = "frequency"
    signal.unit = "Hz"
    return message