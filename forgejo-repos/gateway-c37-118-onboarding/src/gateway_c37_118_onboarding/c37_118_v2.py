"""Bounded IEEE C37.118.2-2011 version-2 frame handling."""

from __future__ import annotations

from dataclasses import dataclass
import math
import struct


SYNC_BYTE = 0xAA
WIRE_VERSION = 2
FRAME_HEADER_BYTES = 14
FRAME_CHECKSUM_BYTES = 2
MIN_FRAME_BYTES = FRAME_HEADER_BYTES + FRAME_CHECKSUM_BYTES
MAX_FRAME_BYTES = 65_535

FRAME_TYPE_DATA = 0
FRAME_TYPE_CONFIGURATION_1 = 2
FRAME_TYPE_CONFIGURATION_2 = 3
FRAME_TYPE_COMMAND = 4

COMMAND_TURN_OFF = 0x0001
COMMAND_TURN_ON = 0x0002
COMMAND_REQUEST_CONFIGURATION_1 = 0x0004
COMMAND_REQUEST_CONFIGURATION_2 = 0x0005


class C37_118V2Error(ValueError):
    """Raised when an incoming v2 frame is malformed or unsupported."""


@dataclass(frozen=True)
class FrameHeader:
    """Shared C37.118 v2 frame header fields."""

    frame_type: int
    idcode: int
    soc: int
    message_time_quality: int
    fracsec: int


@dataclass(frozen=True)
class PhasorConfiguration:
    """One CFG-2 phasor entry used to decode a matching data block."""

    channel_name: str
    is_current: bool
    integer_scale: float


@dataclass(frozen=True)
class PmuConfiguration:
    """One PMU block from a C37.118 v2 CFG-2 frame."""

    idcode: int
    station_name: str
    phasors: tuple[PhasorConfiguration, ...]
    analog_count: int
    digital_word_count: int
    phasor_is_float: bool
    phasor_is_polar: bool
    frequency_is_float: bool
    analog_is_float: bool
    nominal_frequency_hz: int
    configuration_count: int


@dataclass(frozen=True)
class ConfigurationFrame:
    """A complete CFG-2 description needed to decode subsequent data frames."""

    header: FrameHeader
    time_base: int
    pmus: tuple[PmuConfiguration, ...]
    data_rate: int

    def pmu(self, idcode: int) -> PmuConfiguration:
        """Return the uniquely configured PMU with the supplied IDCODE."""

        for configuration in self.pmus:
            if configuration.idcode == idcode:
                return configuration
        raise C37_118V2Error(f"CFG-2 does not contain PMU IDCODE {idcode}")


@dataclass(frozen=True)
class PmuData:
    """Decoded scalar data for one PMU block in a v2 data frame."""

    idcode: int
    stat: int
    phasor_magnitudes: tuple[float, ...]
    frequency_hz: float
    rocof_hz_per_s: float


@dataclass(frozen=True)
class DataFrame:
    """A decoded C37.118 v2 data frame."""

    header: FrameHeader
    pmus: tuple[PmuData, ...]

    def pmu(self, idcode: int) -> PmuData:
        """Return the uniquely decoded PMU block with the supplied IDCODE."""

        for data in self.pmus:
            if data.idcode == idcode:
                return data
        raise C37_118V2Error(f"Data frame does not contain PMU IDCODE {idcode}")


class FrameBuffer:
    """Incrementally split a TCP byte stream into bounded, verified frames."""

    def __init__(self, maximum_frame_bytes: int = MAX_FRAME_BYTES) -> None:
        if not MIN_FRAME_BYTES <= maximum_frame_bytes <= MAX_FRAME_BYTES:
            raise ValueError(
                f"maximum_frame_bytes must be in {MIN_FRAME_BYTES}..={MAX_FRAME_BYTES}"
            )
        self._maximum_frame_bytes = maximum_frame_bytes
        self._buffer = bytearray()

    @property
    def pending_bytes(self) -> int:
        """Return the incomplete frame bytes retained from a prior TCP read."""

        return len(self._buffer)

    def feed(self, received: bytes) -> tuple[bytes, ...]:
        """Consume one TCP read without retaining more than one frame at once."""

        frames: list[bytes] = []
        received_offset = 0
        while received_offset < len(received):
            expected_size = self._expected_size()
            if expected_size is None:
                bytes_needed = 4 - len(self._buffer)
            else:
                bytes_needed = expected_size - len(self._buffer)

            take_count = min(bytes_needed, len(received) - received_offset)
            self._buffer.extend(received[received_offset : received_offset + take_count])
            received_offset += take_count

            expected_size = self._expected_size()
            if expected_size is not None and len(self._buffer) == expected_size:
                frame = bytes(self._buffer)
                parse_frame_header(frame, self._maximum_frame_bytes)
                frames.append(frame)
                self._buffer.clear()
        return tuple(frames)

    def _expected_size(self) -> int | None:
        if len(self._buffer) < 4:
            return None
        if self._buffer[0] != SYNC_BYTE:
            raise C37_118V2Error("frame does not begin with the C37.118 sync byte")
        frame_version = self._buffer[1] & 0x0F
        if self._buffer[1] & 0x80 or frame_version != WIRE_VERSION:
            raise C37_118V2Error("frame is not a C37.118 version-2 message")
        frame_size = int.from_bytes(self._buffer[2:4], "big")
        if frame_size < MIN_FRAME_BYTES or frame_size > self._maximum_frame_bytes:
            raise C37_118V2Error("frame declares an unsafe size")
        return frame_size


class _FrameCursor:
    """Bounds-checked payload reader that excludes the frame checksum."""

    def __init__(self, frame: bytes) -> None:
        self._frame = frame
        self._offset = FRAME_HEADER_BYTES
        self._payload_end = len(frame) - FRAME_CHECKSUM_BYTES

    def read_u16(self) -> int:
        return int.from_bytes(self.read_bytes(2), "big")

    def read_i16(self) -> int:
        return struct.unpack(">h", self.read_bytes(2))[0]

    def read_u32(self) -> int:
        return int.from_bytes(self.read_bytes(4), "big")

    def read_f32(self) -> float:
        value = struct.unpack(">f", self.read_bytes(4))[0]
        if not math.isfinite(value):
            raise C37_118V2Error("frame contains a non-finite floating-point value")
        return value

    def read_fixed_ascii_name(self) -> str:
        raw_name = self.read_bytes(16).rstrip(b" \x00")
        if b"\x00" in raw_name:
            raise C37_118V2Error("CFG-2 name contains an embedded NUL byte")
        try:
            return raw_name.decode("ascii")
        except UnicodeDecodeError as error:
            raise C37_118V2Error("CFG-2 name is not ASCII") from error

    def read_bytes(self, count: int) -> bytes:
        if count < 0 or self._offset + count > self._payload_end:
            raise C37_118V2Error("frame ends before its declared payload is complete")
        value = self._frame[self._offset : self._offset + count]
        self._offset += count
        return value

    def require_consumed(self) -> None:
        if self._offset != self._payload_end:
            raise C37_118V2Error("frame contains unexpected trailing payload bytes")


def crc16_ccitt(payload: bytes) -> int:
    """Calculate the C37.118 CRC-CCITT checksum with the v2 seed and polynomial."""

    checksum = 0xFFFF
    for byte_value in payload:
        checksum ^= byte_value << 8
        for _ in range(8):
            if checksum & 0x8000:
                checksum = ((checksum << 1) ^ 0x1021) & 0xFFFF
            else:
                checksum = (checksum << 1) & 0xFFFF
    return checksum


def parse_frame_header(
    frame: bytes,
    maximum_frame_bytes: int = MAX_FRAME_BYTES,
) -> FrameHeader:
    """Validate a complete v2 envelope and return its common header fields."""

    if not MIN_FRAME_BYTES <= maximum_frame_bytes <= MAX_FRAME_BYTES:
        raise ValueError(
            f"maximum_frame_bytes must be in {MIN_FRAME_BYTES}..={MAX_FRAME_BYTES}"
        )
    if len(frame) < MIN_FRAME_BYTES:
        raise C37_118V2Error("frame is shorter than the C37.118 envelope")
    if frame[0] != SYNC_BYTE:
        raise C37_118V2Error("frame does not begin with the C37.118 sync byte")

    sync_and_type = frame[1]
    if sync_and_type & 0x80 or sync_and_type & 0x0F != WIRE_VERSION:
        raise C37_118V2Error("frame is not a C37.118 version-2 message")
    frame_size = int.from_bytes(frame[2:4], "big")
    if frame_size < MIN_FRAME_BYTES or frame_size > maximum_frame_bytes:
        raise C37_118V2Error("frame declares an unsafe size")
    if len(frame) != frame_size:
        raise C37_118V2Error("frame bytes do not match the declared size")

    expected_checksum = int.from_bytes(frame[-FRAME_CHECKSUM_BYTES:], "big")
    if crc16_ccitt(frame[:-FRAME_CHECKSUM_BYTES]) != expected_checksum:
        raise C37_118V2Error("frame checksum does not match its payload")

    fracsec_word = int.from_bytes(frame[10:14], "big")
    return FrameHeader(
        frame_type=(sync_and_type >> 4) & 0x07,
        idcode=int.from_bytes(frame[4:6], "big"),
        soc=int.from_bytes(frame[6:10], "big"),
        message_time_quality=(fracsec_word >> 24) & 0xFF,
        fracsec=fracsec_word & 0x00FF_FFFF,
    )


def parse_configuration_2(frame: bytes) -> ConfigurationFrame:
    """Decode a complete C37.118 version-2 CFG-2 frame."""

    header = parse_frame_header(frame)
    if header.frame_type != FRAME_TYPE_CONFIGURATION_2:
        raise C37_118V2Error("frame is not a C37.118 CFG-2 message")

    cursor = _FrameCursor(frame)
    time_base = cursor.read_u32()
    if time_base == 0:
        raise C37_118V2Error("CFG-2 TIME_BASE must be non-zero")
    pmu_count = cursor.read_u16()
    if pmu_count == 0:
        raise C37_118V2Error("CFG-2 must contain at least one PMU")

    pmus: list[PmuConfiguration] = []
    pmu_idcodes: set[int] = set()
    for _ in range(pmu_count):
        pmu = _parse_pmu_configuration(cursor)
        if pmu.idcode in pmu_idcodes:
            raise C37_118V2Error("CFG-2 repeats a PMU IDCODE")
        pmu_idcodes.add(pmu.idcode)
        pmus.append(pmu)

    data_rate = cursor.read_i16()
    if data_rate == 0:
        raise C37_118V2Error("CFG-2 DATA_RATE must not be zero")
    cursor.require_consumed()
    return ConfigurationFrame(
        header=header,
        time_base=time_base,
        pmus=tuple(pmus),
        data_rate=data_rate,
    )


def decode_data_frame(frame: bytes, configuration: ConfigurationFrame) -> DataFrame:
    """Decode a v2 data frame using its previously accepted CFG-2 layout."""

    header = parse_frame_header(frame)
    if header.frame_type != FRAME_TYPE_DATA:
        raise C37_118V2Error("frame is not a C37.118 data message")
    if header.fracsec >= configuration.time_base:
        raise C37_118V2Error("data-frame FRACSEC exceeds CFG-2 TIME_BASE")

    cursor = _FrameCursor(frame)
    pmus = tuple(_decode_pmu_data(cursor, pmu) for pmu in configuration.pmus)
    cursor.require_consumed()
    return DataFrame(header=header, pmus=pmus)


def encode_command(
    idcode: int,
    command: int,
    soc: int,
    fracsec: int = 0,
    message_time_quality: int = 0,
) -> bytes:
    """Build one validated legacy v2 command frame."""

    if not 1 <= idcode <= 65_535:
        raise ValueError("idcode must be in 1..=65535")
    if command not in {
        COMMAND_TURN_OFF,
        COMMAND_TURN_ON,
        COMMAND_REQUEST_CONFIGURATION_1,
        COMMAND_REQUEST_CONFIGURATION_2,
    }:
        raise ValueError("command is not supported by the v2 gateway")
    if not 0 <= soc <= 0xFFFF_FFFF:
        raise ValueError("soc must fit uint32")
    if not 0 <= fracsec <= 0x00FF_FFFF:
        raise ValueError("fracsec must fit the v2 24-bit fraction")
    if not 0 <= message_time_quality <= 0xFF:
        raise ValueError("message_time_quality must fit uint8")

    fracsec_word = (message_time_quality << 24) | fracsec
    frame_size = FRAME_HEADER_BYTES + 2 + FRAME_CHECKSUM_BYTES
    frame = bytearray(
        struct.pack(
            ">BBHHIIH",
            SYNC_BYTE,
            (FRAME_TYPE_COMMAND << 4) | WIRE_VERSION,
            frame_size,
            idcode,
            soc,
            fracsec_word,
            command,
        )
    )
    frame.extend(struct.pack(">H", crc16_ccitt(frame)))
    return bytes(frame)


def _parse_pmu_configuration(cursor: _FrameCursor) -> PmuConfiguration:
    station_name = cursor.read_fixed_ascii_name()
    idcode = cursor.read_u16()
    format_word = cursor.read_u16()
    if format_word & ~0x000F:
        raise C37_118V2Error("CFG-2 FORMAT has non-zero reserved bits")
    phasor_count = cursor.read_u16()
    analog_count = cursor.read_u16()
    digital_word_count = cursor.read_u16()

    channel_count = phasor_count + analog_count + 16 * digital_word_count
    channel_names = tuple(cursor.read_fixed_ascii_name() for _ in range(channel_count))
    phasor_units = tuple(cursor.read_u32() for _ in range(phasor_count))
    cursor.read_bytes(4 * analog_count)
    cursor.read_bytes(4 * digital_word_count)
    nominal_frequency_word = cursor.read_u16()
    if nominal_frequency_word & ~0x0001:
        raise C37_118V2Error("CFG-2 FNOM has non-zero reserved bits")
    configuration_count = cursor.read_u16()

    phasors = tuple(
        PhasorConfiguration(
            channel_name=channel_names[index],
            is_current=bool(phasor_units[index] & 0x8000_0000),
            integer_scale=(phasor_units[index] & 0x00FF_FFFF) * 0.00001,
        )
        for index in range(phasor_count)
    )
    return PmuConfiguration(
        idcode=idcode,
        station_name=station_name,
        phasors=phasors,
        analog_count=analog_count,
        digital_word_count=digital_word_count,
        phasor_is_float=bool(format_word & 0x0002),
        phasor_is_polar=bool(format_word & 0x0001),
        frequency_is_float=bool(format_word & 0x0008),
        analog_is_float=bool(format_word & 0x0004),
        nominal_frequency_hz=50 if nominal_frequency_word & 0x0001 else 60,
        configuration_count=configuration_count,
    )


def _decode_pmu_data(cursor: _FrameCursor, configuration: PmuConfiguration) -> PmuData:
    stat = cursor.read_u16()
    phasor_magnitudes = tuple(
        _decode_phasor_magnitude(cursor, configuration, phasor)
        for phasor in configuration.phasors
    )
    if configuration.frequency_is_float:
        frequency_hz = cursor.read_f32()
        rocof_hz_per_s = cursor.read_f32()
    else:
        frequency_hz = configuration.nominal_frequency_hz + cursor.read_i16() / 1_000.0
        rocof_hz_per_s = cursor.read_i16() / 100.0

    for _ in range(configuration.analog_count):
        if configuration.analog_is_float:
            cursor.read_f32()
        else:
            cursor.read_i16()
    cursor.read_bytes(2 * configuration.digital_word_count)
    return PmuData(
        idcode=configuration.idcode,
        stat=stat,
        phasor_magnitudes=phasor_magnitudes,
        frequency_hz=frequency_hz,
        rocof_hz_per_s=rocof_hz_per_s,
    )


def _decode_phasor_magnitude(
    cursor: _FrameCursor,
    configuration: PmuConfiguration,
    phasor: PhasorConfiguration,
) -> float:
    if configuration.phasor_is_float:
        first_value = cursor.read_f32()
        second_value = cursor.read_f32()
        if configuration.phasor_is_polar:
            if first_value < 0:
                raise C37_118V2Error("data frame has a negative polar phasor magnitude")
            return first_value
        return math.hypot(first_value, second_value)

    if configuration.phasor_is_polar:
        magnitude = cursor.read_u16()
        cursor.read_i16()
        return magnitude * phasor.integer_scale
    real_component = cursor.read_i16()
    imaginary_component = cursor.read_i16()
    return math.hypot(real_component, imaginary_component) * phasor.integer_scale