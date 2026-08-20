"""HTTP and WebSocket behavior for the on-demand IEC 104 browser."""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import Event
import unittest

from fastapi.testclient import TestClient

from iec104_browser.app import create_app
from iec104_browser.events import MonitorEvent, MonitorStatus
from iec104_browser.hub import LiveHub


@dataclass
class _FakeMonitor:
    event_callback: object
    status_callback: object
    started: bool = False
    stopped: bool = False
    stopped_event: Event = field(default_factory=Event)

    def start(self) -> None:
        self.started = True
        self.status_callback(MonitorStatus(active=True, state="active"))

    def stop(self) -> None:
        self.stopped = True
        self.stopped_event.set()
        self.status_callback(MonitorStatus(active=False, state="idle"))

    def emit(self, event: MonitorEvent) -> None:
        self.event_callback(event)


class BrowserApiTests(unittest.TestCase):
    """Expose a live stream without retaining messages after it closes."""

    def setUp(self) -> None:
        self.monitors: list[_FakeMonitor] = []

        def factory(event_callback, status_callback):
            monitor = _FakeMonitor(event_callback, status_callback)
            self.monitors.append(monitor)
            return monitor

        self.hub = LiveHub(factory)
        self.app = create_app(self.hub)

    def test_health_and_idle_status_do_not_start_iec(self) -> None:
        with TestClient(self.app) as client:
            health = client.get("/healthz")
            status = client.get("/v1/iec104/status")

        self.assertEqual(health.json(), {"status": "ok"})
        self.assertEqual(status.json(), {"active": False, "state": "idle", "viewers": 0})
        self.assertEqual(self.monitors, [])

    def test_websocket_receives_current_values_then_releases_iec(self) -> None:
        with TestClient(self.app) as client:
            with client.websocket_connect("/v1/iec104/live") as websocket:
                connecting = websocket.receive_json()
                active = websocket.receive_json()
                self.assertEqual(connecting["state"], "connecting")
                self.assertEqual(active, {"kind": "status", "active": True, "state": "active", "viewers": 1})
                self.monitors[0].emit(_event())
                message = websocket.receive_json()

                self.assertEqual(message, _event().payload())
                websocket.close()
                self.assertTrue(self.monitors[0].stopped_event.wait(1))
            status = client.get("/v1/iec104/status")

        self.assertTrue(self.monitors[0].stopped)
        self.assertEqual(status.json(), {"active": False, "state": "idle", "viewers": 0})


def _event() -> MonitorEvent:
    return MonitorEvent(
        cause_code=3,
        cause_name="SPONTANEOUS",
        common_address=1,
        information_object_address=2,
        quality_flags=("valid",),
        quality_value=0,
        received_at="2026-08-20T12:00:00.000Z",
        type_id="M_ME_NC_1",
        value=50.01,
        value_text="50.01",
    )


if __name__ == "__main__":
    unittest.main()