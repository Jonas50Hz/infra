"""Dynamic c104 monitor tests without a networked controlled station."""

from __future__ import annotations

from types import SimpleNamespace
import unittest

import c104

from iec104_browser.events import MonitorStatus
from iec104_browser.monitor import Iec104Monitor, MonitorError


class _FakePoint:
    def __init__(self, station: "_FakeStation", io_address: int, point_type: c104.Type) -> None:
        self.station = station
        self.io_address = io_address
        self.type = point_type
        self.quality = SimpleNamespace(value=32)
        self.value = True
        self.receive_callback = None

    def on_receive(self, callback) -> None:
        self.receive_callback = callback


class _FakeStation:
    def __init__(self, common_address: int) -> None:
        self.common_address = common_address
        self.points: list[_FakePoint] = []

    def add_point(self, io_address: int, type: c104.Type) -> _FakePoint:
        point = _FakePoint(self, io_address, type)
        self.points.append(point)
        return point


class _FakeConnection:
    def __init__(self) -> None:
        self.stations: list[_FakeStation] = []

    def add_station(self, common_address: int) -> _FakeStation:
        station = _FakeStation(common_address)
        self.stations.append(station)
        return station


class _FakeClient:
    def __init__(self) -> None:
        self.has_active_connections = True
        self.is_running = False
        self.connection = _FakeConnection()
        self.new_point_callback = None
        self.new_station_callback = None
        self.request = None

    def on_new_station(self, callback) -> None:
        self.new_station_callback = callback

    def on_new_point(self, callback) -> None:
        self.new_point_callback = callback

    def add_connection(self, ip: str, port: int, init: object) -> _FakeConnection:
        self.request = (ip, port, init)
        return self.connection

    def start(self) -> None:
        self.is_running = True

    def stop(self) -> None:
        self.is_running = False


class _FakeC104:
    Init = SimpleNamespace(NONE="none")

    def __init__(self) -> None:
        self.client = _FakeClient()

    def Client(self) -> _FakeClient:
        return self.client


class Iec104MonitorTests(unittest.TestCase):
    """Register unknown c104 stations and points without retaining past values."""

    def test_discovers_first_station_and_point_then_emits_normalized_value(self) -> None:
        events = []
        statuses = []
        fake_c104 = _FakeC104()
        monitor = Iec104Monitor(
            "iec104-exporter",
            2404,
            events.append,
            statuses.append,
            c104_module=fake_c104,
            resolver=lambda host: "172.18.0.20" if host == "iec104-exporter" else "",
            status_poll_seconds=60,
        )

        monitor.start()
        fake_c104.client.new_station_callback(fake_c104.client, fake_c104.client.connection, 17)
        station = fake_c104.client.connection.stations[0]
        fake_c104.client.new_point_callback(fake_c104.client, station, 2001, c104.Type.M_SP_NA_1)
        point = station.points[0]
        point.receive_callback(
            point,
            SimpleNamespace(),
            SimpleNamespace(cot=SimpleNamespace(value=3, name="SPONTANEOUS")),
        )
        monitor.stop()

        self.assertEqual(fake_c104.client.request, ("172.18.0.20", 2404, "none"))
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].common_address, 17)
        self.assertEqual(events[0].information_object_address, 2001)
        self.assertEqual(events[0].type_id, "M_SP_NA_1")
        self.assertTrue(events[0].value)
        self.assertEqual(events[0].quality_flags, ("substituted",))
        self.assertIn(MonitorStatus(active=True, state="active"), statuses)
        self.assertEqual(statuses[-1], MonitorStatus(active=False, state="idle"))

    def test_dynamic_callbacks_keep_concrete_c104_annotations(self) -> None:
        monitor = Iec104Monitor(
            "127.0.0.1",
            2404,
            lambda event: None,
            lambda status: None,
            c104_module=_FakeC104(),
            status_poll_seconds=60,
        )
        station_callback = monitor._new_station_callback()
        point_callback = monitor._new_point_callback()
        receive_callback = monitor._point_receive_callback()

        self.assertIs(station_callback.__annotations__["client"], c104.Client)
        self.assertIs(station_callback.__annotations__["connection"], c104.Connection)
        self.assertIs(point_callback.__annotations__["station"], c104.Station)
        self.assertIs(point_callback.__annotations__["point_type"], c104.Type)
        self.assertIs(receive_callback.__annotations__["point"], c104.Point)
        self.assertIs(receive_callback.__annotations__["message"], c104.IncomingMessage)
        self.assertIs(receive_callback.__annotations__["return"], c104.ResponseState)

    def test_rejects_unresolvable_exporter_host(self) -> None:
        monitor = Iec104Monitor(
            "unavailable",
            2404,
            lambda event: None,
            lambda status: None,
            c104_module=_FakeC104(),
            resolver=lambda host: (_ for _ in ()).throw(OSError("missing")),
            status_poll_seconds=60,
        )

        with self.assertRaisesRegex(MonitorError, "address is unavailable"):
            monitor.start()


if __name__ == "__main__":
    unittest.main()