"""c104 adapter for one-way monitor-direction IEC 104 delivery."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol

import c104

from iec104_common.generated import iec104_export_pb2
from iec104_exporter.ingress import Iec104IngressGuard


class Iec104TransportError(RuntimeError):
    """Raised when a validated Export record cannot be delivered by c104."""


class Iec104Transport(Protocol):
    """Minimal transport contract used by the Kafka worker."""

    @property
    def active_control_center(self) -> bool:
        """Whether one control center has started IEC 104 data transfer."""

    def publish(self, record: iec104_export_pb2.ExportRecord) -> bool:
        """Attempt delivery and return whether c104 accepted the batch."""


_C104_TYPES = {
    iec104_export_pb2.IEC104_TYPE_ID_M_SP_NA_1: "M_SP_NA_1",
    iec104_export_pb2.IEC104_TYPE_ID_M_DP_NA_1: "M_DP_NA_1",
    iec104_export_pb2.IEC104_TYPE_ID_M_ME_NC_1: "M_ME_NC_1",
}
_QUALITY_BITS = (
    ("invalid", "Invalid"),
    ("not_topical", "NonTopical"),
    ("substituted", "Substituted"),
    ("blocked", "Blocked"),
    ("overflow", "Overflow"),
)


class C104Transport:
    """Expose validated Export records as c104 server-side monitor batches."""

    def __init__(
        self,
        bind_host: str,
        port: int,
        backend_port: int = 2405,
        c104_module: Any | None = None,
        ingress_guard_factory: Callable[[str, int, str, int], Any] = Iec104IngressGuard,
    ) -> None:
        self._backend_port = backend_port
        self._bind_host = bind_host
        self._ingress_guard_factory = ingress_guard_factory
        self._port = port
        self._c104 = c104 if c104_module is None else c104_module
        self._points: dict[tuple[int, int], tuple[int, Any]] = {}
        self._ingress_guard: Any | None = None
        self._server: Any | None = None
        self._stations: dict[int, Any] = {}

    @property
    def listener_ready(self) -> bool:
        """Whether c104 has started the controlled-station listener."""

        return (
            self._server is not None
            and self._ingress_guard is not None
            and bool(self._server.is_running)
            and bool(self._ingress_guard.ready)
        )

    @property
    def active_control_center(self) -> bool:
        """Whether c104 reports an active control-center data connection."""

        return self.listener_ready and bool(self._server.has_active_connections)

    def start(self) -> None:
        """Start one c104 controlled station that accepts one control center."""

        if self._server is not None:
            return
        server = self._c104.Server(
            ip="127.0.0.1",
            port=self._backend_port,
            max_connections=1,
        )
        server.start()
        if not server.is_running:
            server.stop()
            raise Iec104TransportError("c104 server did not start its listener")
        guard = self._ingress_guard_factory(
            self._bind_host,
            self._port,
            "127.0.0.1",
            self._backend_port,
        )
        try:
            guard.start()
        except OSError as error:
            server.stop()
            raise Iec104TransportError("IEC 104 ingress guard did not start") from error
        self._server = server
        self._ingress_guard = guard

    def stop(self) -> None:
        """Stop c104 and forget dynamic stations and point registrations."""

        server = self._server
        ingress_guard = self._ingress_guard
        self._server = None
        self._ingress_guard = None
        self._stations.clear()
        self._points.clear()
        if ingress_guard is not None:
            ingress_guard.stop()
        if server is not None and server.is_running:
            server.stop()

    def publish(self, record: iec104_export_pb2.ExportRecord) -> bool:
        """Publish one validated ASDU without handling incoming application data."""

        if not self.active_control_center:
            return False
        server = self._require_server()
        asdu = record.iec104_asdu
        c104_type_name = _C104_TYPES.get(asdu.type_id)
        if c104_type_name is None:
            raise Iec104TransportError("validated Export record has an unsupported IEC type")
        station = self._station(asdu.common_address)
        points = [
            self._point(station, asdu.common_address, information_object, asdu.type_id, c104_type_name)
            for information_object in asdu.information_objects
        ]
        batch = self._c104.Batch(
            cause=self._c104.Cot(asdu.cause.code),
            points=points,
        )
        try:
            return bool(server.transmit_batch(batch))
        except (RuntimeError, ValueError) as error:
            raise Iec104TransportError("c104 rejected the outbound monitor batch") from error

    def _require_server(self) -> Any:
        if self._server is None:
            raise Iec104TransportError("c104 server is not running")
        return self._server

    def _station(self, common_address: int) -> Any:
        station = self._stations.get(common_address)
        if station is not None:
            return station
        station = self._require_server().add_station(common_address=common_address)
        if station is None:
            raise Iec104TransportError(f"c104 rejected common address {common_address}")
        self._stations[common_address] = station
        return station

    def _point(
        self,
        station: Any,
        common_address: int,
        information_object: iec104_export_pb2.Iec104InformationObject,
        type_id: int,
        c104_type_name: str,
    ) -> Any:
        point_key = (common_address, information_object.information_object_address)
        existing = self._points.get(point_key)
        if existing is not None:
            existing_type_id, point = existing
            if existing_type_id != type_id:
                raise Iec104TransportError(
                    "information-object address changed IEC type within one controlled station"
                )
        else:
            try:
                point = station.add_point(
                    io_address=information_object.information_object_address,
                    type=getattr(self._c104.Type, c104_type_name),
                )
            except ValueError as error:
                raise Iec104TransportError("c104 rejected the information-object address") from error
            if point is None:
                raise Iec104TransportError("c104 did not create the information object")
            self._points[point_key] = (type_id, point)

        value_field = information_object.WhichOneof("value")
        if value_field == "single_point":
            point.value = information_object.single_point.value
            point.quality = self._quality(information_object.single_point.quality)
        elif value_field == "double_point":
            point.value = self._c104.Double(information_object.double_point.value)
            point.quality = self._quality(information_object.double_point.quality)
        elif value_field == "short_float":
            point.value = information_object.short_float.value
            point.quality = self._quality(information_object.short_float.quality)
        else:
            raise Iec104TransportError("validated Export record has no supported information value")
        return point

    def _quality(self, quality: iec104_export_pb2.Iec104Quality) -> Any:
        result = self._c104.Quality()
        for field_name, c104_name in _QUALITY_BITS:
            if getattr(quality, field_name):
                result = result | getattr(self._c104.Quality, c104_name)
        return result