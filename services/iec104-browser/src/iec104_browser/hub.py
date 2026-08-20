"""Transient WebSocket fan-out with no server-side message history."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import uuid4

from iec104_browser.events import MonitorEvent, MonitorStatus


class MonitorProtocol(Protocol):
    """Minimal lifecycle behavior the live hub requires from an IEC monitor."""

    def start(self) -> None:
        """Open the IEC 104 control-center connection."""

    def stop(self) -> None:
        """Close the IEC 104 control-center connection."""


MonitorFactory = Callable[
    [Callable[[MonitorEvent], None], Callable[[MonitorStatus], None]],
    MonitorProtocol,
]


@dataclass(frozen=True)
class Subscription:
    """A per-page transient event stream."""

    queue: asyncio.Queue[dict[str, object]]
    token: str


class LiveHub:
    """Open c104 for active pages only and discard all state after the last one."""

    def __init__(self, monitor_factory: MonitorFactory, queue_size: int = 256) -> None:
        self._lock = asyncio.Lock()
        self._loop: asyncio.AbstractEventLoop | None = None
        self._monitor: MonitorProtocol | None = None
        self._monitor_factory = monitor_factory
        self._queue_size = queue_size
        self._state = MonitorStatus(active=False, state="idle")
        self._subscriptions: dict[str, asyncio.Queue[dict[str, object]]] = {}

    async def subscribe(self) -> Subscription:
        """Create one browser stream and start IEC 104 for the first viewer."""

        monitor_to_start: MonitorProtocol | None = None
        subscription: Subscription
        async with self._lock:
            queue: asyncio.Queue[dict[str, object]] = asyncio.Queue(maxsize=self._queue_size)
            subscription = Subscription(queue=queue, token=uuid4().hex)
            self._subscriptions[subscription.token] = queue
            self._loop = asyncio.get_running_loop()
            if self._monitor is None:
                self._state = MonitorStatus(active=False, state="connecting")
                monitor_to_start = self._monitor_factory(self._on_event, self._on_status)
                self._monitor = monitor_to_start
            _enqueue(queue, self._status_payload())

        if monitor_to_start is not None:
            try:
                await asyncio.to_thread(monitor_to_start.start)
            except Exception:
                async with self._lock:
                    self._subscriptions.pop(subscription.token, None)
                    if self._monitor is monitor_to_start:
                        self._monitor = None
                        self._state = MonitorStatus(active=False, state="idle")
                        if not self._subscriptions:
                            self._loop = None
                raise
        return subscription

    async def unsubscribe(self, subscription: Subscription) -> None:
        """Drop a page stream and stop IEC 104 after the final page closes."""

        monitor_to_stop: MonitorProtocol | None = None
        async with self._lock:
            self._subscriptions.pop(subscription.token, None)
            if not self._subscriptions and self._monitor is not None:
                monitor_to_stop = self._monitor
                self._monitor = None
                self._loop = None
                self._state = MonitorStatus(active=False, state="idle")
        if monitor_to_stop is not None:
            await asyncio.to_thread(monitor_to_stop.stop)

    async def shutdown(self) -> None:
        """Release c104 during application shutdown without retaining events."""

        monitor_to_stop: MonitorProtocol | None = None
        async with self._lock:
            self._subscriptions.clear()
            monitor_to_stop = self._monitor
            self._monitor = None
            self._loop = None
            self._state = MonitorStatus(active=False, state="idle")
        if monitor_to_stop is not None:
            await asyncio.to_thread(monitor_to_stop.stop)

    async def status(self) -> dict[str, object]:
        """Return live lifecycle state without retaining received ASDU values."""

        async with self._lock:
            return {
                "active": self._state.active,
                "state": self._state.state,
                "viewers": len(self._subscriptions),
            }

    def _on_event(self, event: MonitorEvent) -> None:
        loop = self._loop
        if loop is not None:
            loop.call_soon_threadsafe(self._broadcast, event.payload())

    def _on_status(self, status: MonitorStatus) -> None:
        loop = self._loop
        if loop is not None:
            loop.call_soon_threadsafe(self._publish_status, status)

    def _broadcast(self, payload: dict[str, object]) -> None:
        for queue in tuple(self._subscriptions.values()):
            _enqueue(queue, payload)

    def _publish_status(self, status: MonitorStatus) -> None:
        if status == self._state:
            return
        self._state = status
        payload = self._status_payload()
        for queue in tuple(self._subscriptions.values()):
            _enqueue(queue, payload)

    def _status_payload(self) -> dict[str, object]:
        return {
            "kind": "status",
            "active": self._state.active,
            "state": self._state.state,
            "viewers": len(self._subscriptions),
        }


def _enqueue(queue: asyncio.Queue[dict[str, object]], payload: dict[str, object]) -> None:
    """Keep a slow page bounded while preserving the newest live event."""

    if queue.full():
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            pass
    queue.put_nowait(payload)