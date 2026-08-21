"""Tests for bounded legacy C37.118 version-2 frame decoding."""

from __future__ import annotations

import struct
import unittest

from gateway_c37_118_onboarding.c37_118_v2 import (
    COMMAND_REQUEST_CONFIGURATION_2,
    C37_118V2Error,
    FRAME_TYPE_COMMAND,
    FRAME_TYPE_CONFIGURATION_2,
    FRAME_TYPE_DATA,
    FrameBuffer,
    crc16_ccitt,
    decode_data_frame,
    encode_command,
    parse_configuration_2,
    parse_frame_header,
)


class C37_118V2Tests(unittest.TestCase):
    """Exercise CFG-2 and DATA decoding independently of Kafka or a gateway."""

    def test_decodes_float_polar_configuration_and_data(self) -> None:
        configuration_frame = _configuration_frame(
            [_pmu_block(1001, "PMU-ONE", ("VL1", "IL1"), 0x000B)]
        )
        configuration = parse_configuration_2(configuration_frame)
        data_frame = _data_frame_float_polar(
            [(0, ((230_000.0, 0.0), (500.0, 0.2)), 50.01, -0.02)]
        )

        decoded = decode_data_frame(data_frame, configuration)

        self.assertEqual(configuration.time_base, 1_000_000)
        self.assertEqual(configuration.pmus[0].phasors[0].channel_name, "VL1")
        self.assertTrue(configuration.pmus[0].phasors[1].is_current)
        self.assertAlmostEqual(decoded.pmu(1001).phasor_magnitudes[0], 230_000.0)
        self.assertAlmostEqual(decoded.pmu(1001).frequency_hz, 50.01, places=5)
        self.assertAlmostEqual(decoded.pmu(1001).rocof_hz_per_s, -0.02)

    def test_decodes_scaled_integer_rectangular_values(self) -> None:
        configuration_frame = _configuration_frame(
            [_pmu_block(1001, "PMU-ONE", ("VL1",), 0x0000, phasor_units=(100_000,))]
        )
        configuration = parse_configuration_2(configuration_frame)
        data_frame = _data_frame_integer_rectangular([(0, ((3, 4),), 10, -2)])

        decoded = decode_data_frame(data_frame, configuration).pmu(1001)

        self.assertAlmostEqual(decoded.phasor_magnitudes[0], 5.0)
        self.assertAlmostEqual(decoded.frequency_hz, 50.01)
        self.assertAlmostEqual(decoded.rocof_hz_per_s, -0.02)

    def test_decodes_multiple_pmu_blocks_without_losing_offsets(self) -> None:
        configuration_frame = _configuration_frame(
            [
                _pmu_block(1001, "PMU-ONE", ("VL1",), 0x000B),
                _pmu_block(1002, "PMU-TWO", ("VL1",), 0x000B),
            ]
        )
        configuration = parse_configuration_2(configuration_frame)
        data_frame = _data_frame_float_polar(
            [
                (0, ((230_000.0, 0.0),), 50.01, 0.0),
                (0, ((231_000.0, 0.0),), 49.99, 0.01),
            ]
        )

        decoded = decode_data_frame(data_frame, configuration)

        self.assertEqual([pmu.idcode for pmu in decoded.pmus], [1001, 1002])
        self.assertAlmostEqual(decoded.pmu(1002).phasor_magnitudes[0], 231_000.0)
        self.assertAlmostEqual(decoded.pmu(1002).frequency_hz, 49.99, places=5)

    def test_buffer_accepts_partial_and_concatenated_tcp_reads(self) -> None:
        configuration_frame = _configuration_frame(
            [_pmu_block(1001, "PMU-ONE", ("VL1",), 0x000B)]
        )
        data_frame = _data_frame_float_polar([(0, ((230_000.0, 0.0),), 50.0, 0.0)])
        buffer = FrameBuffer()

        self.assertEqual(buffer.feed(configuration_frame[:7]), ())
        frames = buffer.feed(configuration_frame[7:] + data_frame)

        self.assertEqual(frames, (configuration_frame, data_frame))
        self.assertEqual(buffer.pending_bytes, 0)

    def test_rejects_invalid_crc_and_wrong_wire_version(self) -> None:
        configuration_frame = bytearray(
            _configuration_frame([_pmu_block(1001, "PMU-ONE", ("VL1",), 0x000B)])
        )
        configuration_frame[-1] ^= 0x01
        with self.assertRaisesRegex(C37_118V2Error, "checksum"):
            parse_configuration_2(bytes(configuration_frame))

        configuration_frame[1] = 0x33
        with self.assertRaisesRegex(C37_118V2Error, "version-2"):
            parse_configuration_2(bytes(configuration_frame))

    def test_encodes_a_v2_cfg2_request_command(self) -> None:
        command = encode_command(1001, COMMAND_REQUEST_CONFIGURATION_2, 1_700_000_000)
        header = parse_frame_header(command)

        self.assertEqual(header.frame_type, FRAME_TYPE_COMMAND)
        self.assertEqual(command[1], 0x42)
        self.assertEqual(int.from_bytes(command[14:16], "big"), COMMAND_REQUEST_CONFIGURATION_2)


def _configuration_frame(pmu_blocks: list[bytes]) -> bytes:
    payload = bytearray()
    payload.extend(struct.pack(">IH", 1_000_000, len(pmu_blocks)))
    for pmu_block in pmu_blocks:
        payload.extend(pmu_block)
    payload.extend(struct.pack(">h", 50))
    return _frame(FRAME_TYPE_CONFIGURATION_2, 1001, bytes(payload))


def _pmu_block(
    idcode: int,
    station_name: str,
    phasor_names: tuple[str, ...],
    format_word: int,
    phasor_units: tuple[int, ...] | None = None,
) -> bytes:
    if phasor_units is None:
        phasor_units = tuple(
            0x8000_0001 if channel_name.startswith("I") else 1
            for channel_name in phasor_names
        )
    block = bytearray()
    block.extend(_fixed_name(station_name))
    block.extend(struct.pack(">HHHHH", idcode, format_word, len(phasor_names), 0, 0))
    for channel_name in phasor_names:
        block.extend(_fixed_name(channel_name))
    for phasor_unit in phasor_units:
        block.extend(struct.pack(">I", phasor_unit))
    block.extend(struct.pack(">HH", 1, 3))
    return bytes(block)


def _data_frame_float_polar(
    pmu_blocks: list[tuple[int, tuple[tuple[float, float], ...], float, float]],
) -> bytes:
    payload = bytearray()
    for stat, phasors, frequency_hz, rocof_hz_per_s in pmu_blocks:
        payload.extend(struct.pack(">H", stat))
        for magnitude, angle in phasors:
            payload.extend(struct.pack(">ff", magnitude, angle))
        payload.extend(struct.pack(">ff", frequency_hz, rocof_hz_per_s))
    return _frame(FRAME_TYPE_DATA, 1001, bytes(payload), fracsec=500_000)


def _data_frame_integer_rectangular(
    pmu_blocks: list[tuple[int, tuple[tuple[int, int], ...], int, int]],
) -> bytes:
    payload = bytearray()
    for stat, phasors, frequency_millihz, rocof_centihz_per_s in pmu_blocks:
        payload.extend(struct.pack(">H", stat))
        for real_component, imaginary_component in phasors:
            payload.extend(struct.pack(">hh", real_component, imaginary_component))
        payload.extend(struct.pack(">hh", frequency_millihz, rocof_centihz_per_s))
    return _frame(FRAME_TYPE_DATA, 1001, bytes(payload), fracsec=500_000)


def _frame(frame_type: int, idcode: int, payload: bytes, fracsec: int = 0) -> bytes:
    frame_size = 14 + len(payload) + 2
    frame = bytearray(
        struct.pack(
            ">BBHHII",
            0xAA,
            (frame_type << 4) | 2,
            frame_size,
            idcode,
            1_700_000_000,
            fracsec,
        )
    )
    frame.extend(payload)
    frame.extend(struct.pack(">H", crc16_ccitt(frame)))
    return bytes(frame)


def _fixed_name(value: str) -> bytes:
    encoded = value.encode("ascii")
    if len(encoded) > 16:
        raise ValueError("fixture name exceeds the C37.118 v2 fixed-width field")
    return encoded.ljust(16, b" ")