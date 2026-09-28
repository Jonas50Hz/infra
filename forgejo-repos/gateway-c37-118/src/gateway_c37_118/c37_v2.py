"""Small strict decoder for the reviewed single-PMU C37.118.2 v2 subset."""

from __future__ import annotations

from dataclasses import dataclass
import struct
from typing import Optional


class C37V2Error(ValueError):
    """Raised for an invalid, unsupported, or mismatched v2 frame."""


MAX_FRAME_BYTES = 4096


@dataclass(frozen=True)
class V2Configuration:
    """CFG-2 fields needed to decode one configured PMU data stream."""

    pmu_idcode: int
    time_base: int
    nominal_frequency: int
    phasors: tuple[tuple[str, float], ...]
    phasor_float: bool
    frequency_float: bool
    data_size: int


@dataclass(frozen=True)
class DataFrame:
    """Decoded raw data values and the source timestamp."""

    source_seconds: int
    source_fraction: int
    stat: int
    phasors: dict[str, float]
    frequency: float
    rocof: float


def crc_ccitt(data: bytes) -> int:
    """Return C37.118 CRC-CCITT (initial 0xffff, no final xor)."""

    crc = 0xFFFF
    for byte in data:
        crc ^= byte << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return crc


def command_frame(pmu_idcode: int, command: int) -> bytes:
    """Build a v2 command frame for the approved endpoint."""

    body = struct.pack(">2sHHIIH", b"\xaa\x42", 18, pmu_idcode, 0, 0, command)
    return body + struct.pack(">H", crc_ccitt(body))


def validate_frame(frame: bytes, expected_type: Optional[int] = None) -> None:
    """Check envelope, size, v2 nibble, optional type, and CRC."""

    if len(frame) < 16 or len(frame) > MAX_FRAME_BYTES:
        raise C37V2Error("invalid C37.118 v2 frame size")
    if frame[0] != 0xAA or frame[1] & 0x0F != 0x02:
        raise C37V2Error("expected C37.118 wire version 2")
    if expected_type is not None and frame[1] != expected_type:
        raise C37V2Error("unexpected C37.118 v2 frame type")
    if struct.unpack_from(">H", frame, 2)[0] != len(frame):
        raise C37V2Error("C37.118 frame size does not match envelope")
    if crc_ccitt(frame[:-2]) != struct.unpack_from(">H", frame, len(frame) - 2)[0]:
        raise C37V2Error("C37.118 frame CRC mismatch")


def parse_cfg2(frame: bytes, expected_idcode: int) -> V2Configuration:
    """Decode exactly one PMU CFG-2 and its fixed-width channel/scaling records."""

    validate_frame(frame, 0x32)
    if struct.unpack_from(">H", frame, 4)[0] != expected_idcode:
        raise C37V2Error("CFG-2 IDCODE does not match source")
    time_base = struct.unpack_from(">I", frame, 14)[0] & 0x00FFFFFF
    number_pmus = struct.unpack_from(">H", frame, 18)[0]
    if time_base == 0 or number_pmus != 1:
        raise C37V2Error("only one-PMU CFG-2 with a time base is supported")
    offset = 20
    _station = frame[offset : offset + 16]
    offset += 16
    pmu_idcode, form, phnmr, annmr, dgnmr = struct.unpack_from(">HHHHH", frame, offset)
    offset += 10
    if pmu_idcode != expected_idcode or annmr or dgnmr:
        raise C37V2Error("CFG-2 PMU shape is unsupported")
    names = []
    for _ in range(phnmr):
        name = frame[offset : offset + 16].rstrip(b" \0").decode("ascii", "strict")
        offset += 16
        if not name:
            raise C37V2Error("CFG-2 has an empty phasor channel")
        names.append(name)
    scales = []
    for _ in range(phnmr):
        unit = struct.unpack_from(">I", frame, offset)[0]
        offset += 4
        scales.append((unit & 0x00FFFFFF) * 0.00001)
    offset += 4 * annmr + 4 * dgnmr
    nominal_word, _config_count = struct.unpack_from(">HH", frame, offset)
    nominal = 50 if nominal_word & 1 else 60
    phasor_float = bool(form & 0x0002)
    frequency_float = bool(form & 0x0008)
    if not form & 0x0001:
        raise C37V2Error("rectangular phasors are outside the reviewed mapping")
    phasor_width = 8 if phasor_float else 4
    scalar_width = 4 if frequency_float else 2
    data_size = 14 + 2 + phnmr * phasor_width + scalar_width * 2 + 2
    return V2Configuration(
        pmu_idcode, time_base, nominal, tuple(zip(names, scales, strict=True)),
        phasor_float, frequency_float, data_size,
    )


def parse_data(frame: bytes, configuration: V2Configuration) -> DataFrame:
    """Decode a data frame using the offsets and scales from its CFG-2."""

    validate_frame(frame, 0x02)
    if len(frame) != configuration.data_size:
        raise C37V2Error("Data frame does not match current CFG-2")
    if struct.unpack_from(">H", frame, 4)[0] != configuration.pmu_idcode:
        raise C37V2Error("Data frame IDCODE does not match CFG-2")
    seconds, fracsec = struct.unpack_from(">II", frame, 6)
    offset = 14
    stat = struct.unpack_from(">H", frame, offset)[0]
    offset += 2
    phasors: dict[str, float] = {}
    for name, scale in configuration.phasors:
        if configuration.phasor_float:
            magnitude, _angle = struct.unpack_from(">ff", frame, offset)
            offset += 8
            phasors[name] = abs(magnitude)
        else:
            magnitude, _angle = struct.unpack_from(">hh", frame, offset)
            offset += 4
            phasors[name] = abs(magnitude * scale)
    if configuration.frequency_float:
        frequency, rocof = struct.unpack_from(">ff", frame, offset)
    else:
        frequency_raw, rocof_raw = struct.unpack_from(">hh", frame, offset)
        frequency = configuration.nominal_frequency + frequency_raw / 1_000
        rocof = rocof_raw / 100
    return DataFrame(seconds, fracsec & 0x00FFFFFF, stat, phasors, frequency, rocof)
