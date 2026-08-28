"""HTTP and WebSocket behavior for the persistent IEC 104 browser."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import json
import socket
from threading import Event, Thread
import unittest
from urllib.request import urlopen

from fastapi.testclient import TestClient
import uvicorn
import websockets

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
    """Expose a live stream while the application retains IEC reception."""

    def setUp(self) -> None:
        self.monitors: list[_FakeMonitor] = []

        def factory(event_callback, status_callback):
            monitor = _FakeMonitor(event_callback, status_callback)
            self.monitors.append(monitor)
            return monitor

        self.hub = LiveHub(factory)
        self.app = create_app(self.hub)

    def test_lifespan_starts_iec_without_viewers(self) -> None:
        with TestClient(self.app) as client:
            health = client.get("/healthz")
            status = client.get("/v1/iec104/status")

        self.assertEqual(health.json(), {"status": "ok"})
        self.assertEqual(status.json(), {"active": True, "state": "active", "viewers": 0})
        self.assertEqual(len(self.monitors), 1)
        self.assertTrue(self.monitors[0].started)
        self.assertTrue(self.monitors[0].stopped)

    def test_websocket_receives_current_values_while_iec_stays_running(self) -> None:
        ready = Event()
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]

        class _ReadyServer(uvicorn.Server):
            async def startup(self, sockets: list[socket.socket] | None = None) -> None:
                await super().startup(sockets=sockets)
                ready.set()

        server = _ReadyServer(
            uvicorn.Config(
                self.app,
                host="127.0.0.1",
                port=port,
                log_level="warning",
                access_log=False,
                lifespan="on",
            )
        )
        server_errors: list[Exception] = []

        def serve() -> None:
            try:
                server.run(sockets=[listener])
            except Exception as error:
                server_errors.append(error)

        server_thread = Thread(
            target=serve,
            name="iec104-browser-test-server",
            daemon=True,
        )
        server_thread.start()

        try:
            if not ready.wait(timeout=5):
                if server_errors:
                    raise server_errors[0]
                if not server_thread.is_alive():
                    self.fail("Uvicorn exited before reporting startup readiness")
                self.fail("Uvicorn did not report startup readiness within five seconds")

            async def assert_live_socket() -> None:
                uri = f"ws://127.0.0.1:{port}/v1/iec104/live"
                async with websockets.connect(
                    uri,
                    open_timeout=5,
                    close_timeout=5,
                ) as websocket:
                    first_status = json.loads(
                        await asyncio.wait_for(websocket.recv(), timeout=5)
                    )
                    if first_status == {
                        "kind": "status",
                        "active": False,
                        "state": "connecting",
                        "viewers": 1,
                    }:
                        active = json.loads(
                            await asyncio.wait_for(websocket.recv(), timeout=5)
                        )
                    else:
                        active = first_status
                    self.assertEqual(
                        active,
                        {"kind": "status", "active": True, "state": "active", "viewers": 1},
                    )
                    self.monitors[0].emit(_event())
                    message = json.loads(
                        await asyncio.wait_for(websocket.recv(), timeout=5)
                    )

                    self.assertEqual(message, _event().payload())
            asyncio.run(assert_live_socket())

            with urlopen(f"http://127.0.0.1:{port}/v1/iec104/status", timeout=5) as response:
                status = json.load(response)

            self.assertEqual(status, {"active": True, "state": "active", "viewers": 0})
            self.assertFalse(self.monitors[0].stopped)
        finally:
            server.should_exit = True
            server_thread.join(timeout=5)
            listener.close()
            server_thread.join(timeout=5)
            if server_thread.is_alive():
                self.fail("Uvicorn server thread did not stop within five seconds")
            if server_errors:
                raise server_errors[0]

        self.assertTrue(self.monitors[0].stopped_event.wait(1))
        self.assertTrue(self.monitors[0].stopped)


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