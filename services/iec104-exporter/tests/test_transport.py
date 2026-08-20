"""c104 adapter tests without a real network peer."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
import unittest

from google.protobuf.timestamp_pb2 import Timestamp

from iec104_common.generated import iec104_export_pb2
from iec104_exporter.transport import C104Transport, Iec104TransportError


class _FakeQuality:
    def __init__(self, bits: int = 0) -> None:
        self.value = bits

    def __or__(self, other: "_FakeQuality") -> "_FakeQuality":
        return _FakeQuality(self.value | other.value)


_FakeQuality.Invalid = _FakeQuality(128)
_FakeQuality.NonTopical = _FakeQuality(64)
_FakeQuality.Substituted = _FakeQuality(32)
_FakeQuality.Blocked = _FakeQuality(16)
_FakeQuality.Overflow = _FakeQuality(1)


class _FakePoint:
    def __init__(self, io_address: int, type: str) -> None:
        self.io_address = io_address
        self.type = type
        self.quality = _FakeQuality()
        self.value = None


class _FakeStation:
    def __init__(self, common_address: int) -> None:
        self.common_address = common_address
        self.points: list[_FakePoint] = []

    def add_point(self, io_address: int, type: str) -> _FakePoint:
        point = _FakePoint(io_address, type)
        self.points.append(point)
        return point


class _FakeServer:
    instances: list["_FakeServer"] = []

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.has_active_connections = False
        self.is_running = False
        self.batches: list[_FakeBatch] = []
        _FakeServer.instances.append(self)

    def add_station(self, common_address: int) -> _FakeStation:
        return _FakeStation(common_address)

    def start(self) -> None:
        self.is_running = True

    def stop(self) -> None:
        self.is_running = False

    def transmit_batch(self, batch: "_FakeBatch") -> bool:
        self.batches.append(batch)
        return True


class _FakeBatch:
    def __init__(self, cause: int, points: list[_FakePoint]) -> None:
        self.cause = cause
        self.points = points


class _FakeIngressGuard:
    instances: list["_FakeIngressGuard"] = []

    def __init__(self, bind_host: str, port: int, backend_host: str, backend_port: int) -> None:
        self.backend_host = backend_host
        self.backend_port = backend_port
        self.bind_host = bind_host
        self.port = port
        self.ready = False
        _FakeIngressGuard.instances.append(self)

    def start(self) -> None:
        self.ready = True

    def stop(self) -> None:
        self.ready = False


_FAKE_C104 = SimpleNamespace(
    Server=_FakeServer,
    Type=SimpleNamespace(M_SP_NA_1="single", M_DP_NA_1="double", M_ME_NC_1="float"),
    Cot=lambda value: value,
    Double=lambda value: ("double", value),
    Quality=_FakeQuality,
    Batch=_FakeBatch,
)


class C104TransportTests(unittest.TestCase):
    """Prove only outbound monitor batches reach c104."""

    def setUp(self) -> None:
        _FakeServer.instances.clear()
        _FakeIngressGuard.instances.clear()
        self.transport = C104Transport(
            "0.0.0.0",
            2404,
            backend_port=2405,
            c104_module=_FAKE_C104,
            ingress_guard_factory=_FakeIngressGuard,
        )
        self.transport.start()
        self.server = _FakeServer.instances[0]

    def test_waits_for_active_control_center(self) -> None:
        self.assertFalse(self.transport.publish(_single_point_record()))
        self.assertEqual(self.server.batches, [])

    def test_transmits_typed_batch_with_cot_and_quality(self) -> None:
        self.server.has_active_connections = True
        record = _single_point_record()
        information_object = record.iec104_asdu.information_objects[0]
        information_object.single_point.quality.invalid = True
        information_object.single_point.quality.substituted = True

        self.assertTrue(self.transport.publish(record))

        batch = self.server.batches[0]
        self.assertEqual(batch.cause, 3)
        self.assertEqual(batch.points[0].type, "single")
        self.assertTrue(batch.points[0].value)
        self.assertEqual(batch.points[0].quality.value, 160)

    def test_binds_c104_only_to_the_internal_backend_port(self) -> None:
        guard = _FakeIngressGuard.instances[0]

        self.assertEqual(self.server.kwargs["ip"], "127.0.0.1")
        self.assertEqual(self.server.kwargs["port"], 2405)
        self.assertEqual((guard.bind_host, guard.port), ("0.0.0.0", 2404))

    def test_rejects_type_change_for_existing_information_object(self) -> None:
        self.server.has_active_connections = True
        self.assertTrue(self.transport.publish(_single_point_record()))
        conflicting = _short_float_record()
        conflicting.iec104_asdu.information_objects[0].information_object_address = 1

        with self.assertRaisesRegex(Iec104TransportError, "changed IEC type"):
            self.transport.publish(conflicting)


def _single_point_record() -> iec104_export_pb2.ExportRecord:
    record = _record(iec104_export_pb2.IEC104_TYPE_ID_M_SP_NA_1)
    information_object = record.iec104_asdu.information_objects.add()
    information_object.information_object_address = 1
    information_object.single_point.value = True
    return record


def _short_float_record() -> iec104_export_pb2.ExportRecord:
    record = _record(iec104_export_pb2.IEC104_TYPE_ID_M_ME_NC_1)
    information_object = record.iec104_asdu.information_objects.add()
    information_object.information_object_address = 2
    information_object.short_float.value = 50.01
    return record


def _record(type_id: int) -> iec104_export_pb2.ExportRecord:
    record = iec104_export_pb2.ExportRecord(
        export_id="4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237",
    )
    timestamp = Timestamp()
    timestamp.FromDatetime(datetime(2026, 8, 19, 12, 0, tzinfo=timezone.utc))
    record.created_at.CopyFrom(timestamp)
    record.iec104_asdu.type_id = type_id
    record.iec104_asdu.common_address = 1
    record.iec104_asdu.cause.code = 3
    return record


if __name__ == "__main__":
    unittest.main()