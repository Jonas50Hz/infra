"""On-demand c104 control center that observes monitor-direction values only."""

from collections.abc import Callable
from datetime import datetime, timezone
import socket
from threading import Event, Lock, Thread
from typing import Any

import c104

from iec104_browser.events import MonitorEvent, MonitorStatus


class MonitorError(RuntimeError):
    """Raised when the browser control center cannot establish IEC 104 reception."""


_TYPE_NAMES = {
    c104.Type.M_SP_NA_1: "M_SP_NA_1",
    c104.Type.M_DP_NA_1: "M_DP_NA_1",
    c104.Type.M_ME_NC_1: "M_ME_NC_1",
}
_QUALITY_FLAGS = (
    (128, "invalid"),
    (64, "not_topical"),
    (32, "substituted"),
    (16, "blocked"),
    (1, "overflow"),
)


class Iec104Monitor:
    """Register dynamic points and send received values to a browser live hub."""

    def __init__(
        self,
        exporter_host: str,
        exporter_port: int,
        event_callback: Callable[[MonitorEvent], None],
        status_callback: Callable[[MonitorStatus], None],
        c104_module: Any | None = None,
        resolver: Callable[[str], str] = socket.gethostbyname,
        status_poll_seconds: float = 0.25,
    ) -> None:
        self._c104 = c104 if c104_module is None else c104_module
        self._client: Any | None = None
        self._event_callback = event_callback
        self._exporter_host = exporter_host
        self._exporter_port = exporter_port
        self._lock = Lock()
        self._resolver = resolver
        self._status_callback = status_callback
        self._status_poll_seconds = status_poll_seconds
        self._watcher: Thread | None = None
        self._watcher_stop = Event()

    @property
    def status(self) -> MonitorStatus:
        """Return whether the c104 control center is connecting or active."""

        with self._lock:
            client = self._client
        if client is None:
            return MonitorStatus(active=False, state="idle")
        if not bool(client.is_running):
            return MonitorStatus(active=False, state="connecting")
        if bool(client.has_active_connections):
            return MonitorStatus(active=True, state="active")
        return MonitorStatus(active=False, state="connecting")

    def start(self) -> None:
        """Start a read-only control center only while a browser is subscribed."""

        with self._lock:
            if self._client is not None:
                return
            client = self._c104.Client()
            client.on_new_station(self._new_station_callback())
            client.on_new_point(self._new_point_callback())
            try:
                connection = client.add_connection(
                    ip=self._resolver(self._exporter_host),
                    port=self._exporter_port,
                    init=self._c104.Init.NONE,
                )
            except (OSError, ValueError) as error:
                raise MonitorError("IEC 104 exporter address is unavailable") from error
            if connection is None:
                raise MonitorError("c104 did not create an IEC 104 exporter connection")
            self._client = client

        try:
            client.start()
        except (RuntimeError, ValueError) as error:
            with self._lock:
                self._client = None
            raise MonitorError("c104 did not start the browser control center") from error

        self._watcher_stop.clear()
        self._watcher = Thread(target=self._watch_status, name="iec104-browser-status", daemon=True)
        self._watcher.start()
        self._status_callback(self.status)

    def stop(self) -> None:
        """Release the IEC 104 connection and discard monitor-side state."""

        self._watcher_stop.set()
        watcher = self._watcher
        self._watcher = None
        if watcher is not None:
            watcher.join(timeout=2)

        with self._lock:
            client = self._client
            self._client = None
        if client is not None and bool(client.is_running):
            client.stop()
        self._status_callback(MonitorStatus(active=False, state="idle"))

    def _new_station_callback(self) -> Callable[[c104.Client, c104.Connection, int], None]:
        def on_new_station(
            client: c104.Client,
            connection: c104.Connection,
            common_address: int,
        ) -> None:
            del client
            connection.add_station(common_address=common_address)

        return on_new_station

    def _new_point_callback(self) -> Callable[[c104.Client, c104.Station, int, c104.Type], None]:
        def on_new_point(
            client: c104.Client,
            station: c104.Station,
            io_address: int,
            point_type: c104.Type,
        ) -> None:
            del client
            if point_type not in _TYPE_NAMES:
                return
            point = station.add_point(io_address=io_address, type=point_type)
            if point is None:
                return
            point.on_receive(self._point_receive_callback())

        return on_new_point

    def _point_receive_callback(
        self,
    ) -> Callable[[c104.Point, c104.Information, c104.IncomingMessage], c104.ResponseState]:
        def on_receive(
            point: c104.Point,
            previous_info: c104.Information,
            message: c104.IncomingMessage,
        ) -> c104.ResponseState:
            del previous_info
            event = _monitor_event(point, message)
            if event is not None:
                self._event_callback(event)
            return c104.ResponseState.NONE

        return on_receive

    def _watch_status(self) -> None:
        previous: MonitorStatus | None = None
        while not self._watcher_stop.wait(self._status_poll_seconds):
            status = self.status
            if status != previous:
                self._status_callback(status)
                previous = status


def _monitor_event(point: c104.Point, message: c104.IncomingMessage) -> MonitorEvent | None:
    type_id = _TYPE_NAMES.get(point.type)
    if type_id is None:
        return None
    quality_value = int(point.quality.value)
    quality_flags = tuple(name for bit, name in _QUALITY_FLAGS if quality_value & bit)
    value, value_text = _value(point, type_id)
    received_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return MonitorEvent(
        cause_code=int(message.cot.value),
        cause_name=str(message.cot.name),
        common_address=int(point.station.common_address),
        information_object_address=int(point.io_address),
        quality_flags=quality_flags or ("valid",),
        quality_value=quality_value,
        received_at=received_at,
        type_id=type_id,
        value=value,
        value_text=value_text,
    )


def _value(point: c104.Point, type_id: str) -> tuple[bool | int | float, str]:
    if type_id == "M_SP_NA_1":
        value = bool(point.value)
        return value, "true" if value else "false"
    if type_id == "M_DP_NA_1":
        state = point.value
        return int(state.value), str(state.name)
    value = float(point.value)
    return value, format(value, ".7g")