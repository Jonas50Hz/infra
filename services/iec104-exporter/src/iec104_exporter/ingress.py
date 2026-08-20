"""TCP guard that permits IEC 104 transport frames but blocks application input."""

from __future__ import annotations

import logging
import socket
from threading import Event, Thread


LOGGER = logging.getLogger(__name__)
MAX_APDU_LENGTH = 253
MIN_APDU_LENGTH = 4
_U_FRAME_CONTROL_FIELDS = frozenset(
    {
        b"\x07\x00\x00\x00",  # STARTDT act
        b"\x0b\x00\x00\x00",  # STARTDT con
        b"\x13\x00\x00\x00",  # STOPDT act
        b"\x23\x00\x00\x00",  # STOPDT con
        b"\x43\x00\x00\x00",  # TESTFR act
        b"\x83\x00\x00\x00",  # TESTFR con
    }
)


class Iec104IngressGuard:
    """Proxy one control-center connection without forwarding inbound I-frames."""

    def __init__(
        self,
        bind_host: str,
        port: int,
        backend_host: str,
        backend_port: int,
    ) -> None:
        self._backend_host = backend_host
        self._backend_port = backend_port
        self._bind_host = bind_host
        self._port = port
        self._listener: socket.socket | None = None
        self._ready = Event()
        self._stop = Event()
        self._thread: Thread | None = None

    @property
    def ready(self) -> bool:
        """Report whether the public IEC 104 TCP listener is accepting peers."""

        return self._ready.is_set()

    def start(self) -> None:
        """Start the public listener before c104 receives any control-center bytes."""

        if self._thread is not None:
            return
        listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            listener.bind((self._bind_host, self._port))
            listener.listen(1)
            listener.settimeout(0.2)
        except OSError:
            listener.close()
            raise
        self._listener = listener
        self._ready.set()
        self._thread = Thread(target=self._run, name="iec104-ingress-guard", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Close the listener and any bridge that is waiting on its sockets."""

        self._stop.set()
        listener = self._listener
        self._listener = None
        self._ready.clear()
        if listener is not None:
            _close_socket(listener)
        if self._thread is not None:
            self._thread.join(timeout=5)
        self._thread = None

    def _run(self) -> None:
        listener = self._listener
        if listener is None:
            return
        while not self._stop.is_set():
            try:
                control_center, _address = listener.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            self._bridge(control_center)

    def _bridge(self, control_center: socket.socket) -> None:
        backend: socket.socket | None = None
        bridge_stop = Event()
        try:
            backend = socket.create_connection((self._backend_host, self._backend_port), timeout=5)
            control_center.settimeout(0.2)
            backend.settimeout(0.2)
            inbound = Thread(
                target=self._forward_control_center,
                args=(control_center, backend, bridge_stop),
                name="iec104-ingress-input",
                daemon=True,
            )
            inbound.start()
            self._forward_backend(backend, control_center, bridge_stop)
            bridge_stop.set()
            _close_socket(backend)
            _close_socket(control_center)
            inbound.join(timeout=1)
        except OSError as error:
            if not self._stop.is_set():
                LOGGER.warning("IEC 104 ingress bridge closed: %s", error)
        finally:
            bridge_stop.set()
            if backend is not None:
                _close_socket(backend)
            _close_socket(control_center)

    def _forward_control_center(
        self,
        control_center: socket.socket,
        backend: socket.socket,
        bridge_stop: Event,
    ) -> None:
        pending = b""
        while not self._stop.is_set() and not bridge_stop.is_set():
            try:
                chunk = control_center.recv(4096)
            except TimeoutError:
                continue
            except OSError:
                bridge_stop.set()
                return
            if not chunk:
                bridge_stop.set()
                return
            frames, pending, rejected = _allowed_transport_frames(pending + chunk)
            if rejected:
                LOGGER.warning("IEC 104 ingress rejected an inbound application or malformed frame")
                bridge_stop.set()
                _close_socket(backend)
                _close_socket(control_center)
                return
            try:
                for frame in frames:
                    backend.sendall(frame)
            except OSError:
                bridge_stop.set()
                return

    def _forward_backend(
        self,
        backend: socket.socket,
        control_center: socket.socket,
        bridge_stop: Event,
    ) -> None:
        while not self._stop.is_set() and not bridge_stop.is_set():
            try:
                chunk = backend.recv(4096)
            except TimeoutError:
                continue
            except OSError:
                bridge_stop.set()
                return
            if not chunk:
                bridge_stop.set()
                return
            try:
                control_center.sendall(chunk)
            except OSError:
                bridge_stop.set()
                return


def _allowed_transport_frames(payload: bytes) -> tuple[tuple[bytes, ...], bytes, bool]:
    """Split complete IEC APDUs and reject every client-to-server I-frame."""

    frames: list[bytes] = []
    offset = 0
    while len(payload) - offset >= 2:
        if payload[offset] != 0x68:
            return tuple(frames), b"", True
        apdu_length = payload[offset + 1]
        if not MIN_APDU_LENGTH <= apdu_length <= MAX_APDU_LENGTH:
            return tuple(frames), b"", True
        frame_end = offset + apdu_length + 2
        if len(payload) < frame_end:
            break
        frame = payload[offset:frame_end]
        if not _is_transport_frame(frame):
            return tuple(frames), b"", True
        frames.append(frame)
        offset = frame_end
    return tuple(frames), payload[offset:], False


def _is_transport_frame(frame: bytes) -> bool:
    """Accept only IEC 104 U frames and structurally valid S frames."""

    if len(frame) != 6:
        return False
    control = frame[2:]
    if control in _U_FRAME_CONTROL_FIELDS:
        return True
    # An S frame is 0x01 0x00 followed by an even receive sequence number.
    return control[0] == 0x01 and control[1] == 0 and control[2] & 0x01 == 0


def _close_socket(connection: socket.socket) -> None:
    try:
        connection.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    connection.close()