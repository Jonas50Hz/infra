"""Transient live-hub lifecycle tests without a real IEC 104 connection."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import unittest

from iec104_browser.events import MonitorEvent, MonitorStatus
from iec104_browser.hub import LiveHub


@dataclass
class _FakeMonitor:
    event_callback: object
    status_callback: object
    started: bool = False
    stopped: bool = False

    def start(self) -> None:
        self.started = True
        self.status_callback(MonitorStatus(active=False, state="connecting"))

    def stop(self) -> None:
        self.stopped = True
        self.status_callback(MonitorStatus(active=False, state="idle"))

    def emit(self, event: MonitorEvent) -> None:
        self.event_callback(event)


class LiveHubTests(unittest.IsolatedAsyncioTestCase):
    """Require monitor lifecycle and message state to follow browser viewers."""

    async def asyncSetUp(self) -> None:
        self.monitors: list[_FakeMonitor] = []

        def factory(event_callback, status_callback):
            monitor = _FakeMonitor(event_callback, status_callback)
            self.monitors.append(monitor)
            return monitor

        self.hub = LiveHub(factory)

    async def asyncTearDown(self) -> None:
        await self.hub.shutdown()

    async def test_first_viewer_starts_monitor_and_last_viewer_stops_it(self) -> None:
        first = await self.hub.subscribe()
        second = await self.hub.subscribe()

        self.assertEqual(len(self.monitors), 1)
        self.assertTrue(self.monitors[0].started)
        self.assertEqual((await self.hub.status())["viewers"], 2)

        await self.hub.unsubscribe(first)
        self.assertFalse(self.monitors[0].stopped)
        await self.hub.unsubscribe(second)

        self.assertTrue(self.monitors[0].stopped)
        self.assertEqual(await self.hub.status(), {"active": False, "state": "idle", "viewers": 0})

    async def test_message_is_broadcast_only_to_open_pages(self) -> None:
        first = await self.hub.subscribe()
        await first.queue.get()
        event = _event()

        self.monitors[0].emit(event)
        payload = await asyncio.wait_for(first.queue.get(), timeout=1)

        self.assertEqual(payload, event.payload())
        await self.hub.unsubscribe(first)

        second = await self.hub.subscribe()
        status = await asyncio.wait_for(second.queue.get(), timeout=1)
        self.assertEqual(status["kind"], "status")
        self.assertTrue(second.queue.empty())

    async def test_slow_page_queue_remains_bounded(self) -> None:
        hub = LiveHub(self._factory, queue_size=1)
        subscription = await hub.subscribe()
        await subscription.queue.get()

        self.monitors[-1].emit(_event("first"))
        self.monitors[-1].emit(_event("second"))
        payload = await asyncio.wait_for(subscription.queue.get(), timeout=1)

        self.assertEqual(payload["value_text"], "second")
        await hub.unsubscribe(subscription)

    def _factory(self, event_callback, status_callback):
        monitor = _FakeMonitor(event_callback, status_callback)
        self.monitors.append(monitor)
        return monitor


def _event(value_text: str = "50.01") -> MonitorEvent:
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
        value_text=value_text,
    )