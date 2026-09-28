"""Byte-level tests for the reviewed C37.118 v2 decoder."""

from __future__ import annotations

import struct
import unittest

from gateway_c37_118.c37_v2 import C37V2Error, crc_ccitt, parse_cfg2, parse_data


def framed(frame_type: int, pmu_id: int, payload: bytes, seconds: int = 100) -> bytes:
    """Build a CRC-protected v2 frame for a decoder test."""

    body = struct.pack(">2sHHII", bytes((0xAA, frame_type)), 14 + len(payload) + 2, pmu_id, seconds, 0)
    body += payload
    return body + struct.pack(">H", crc_ccitt(body))


def cfg2() -> bytes:
    """One fixed-point polar six-channel PMU configuration."""

    channels = ("VA", "VB", "VC", "IA", "IB", "IC")
    payload = struct.pack(">IH", 1_000_000, 1)
    payload += b"PMU".ljust(16, b" ") + struct.pack(">HHHHH", 1001, 1, 6, 0, 0)
    payload += b"".join(name.encode().ljust(16, b" ") for name in channels)
    payload += b"".join(struct.pack(">I", 100_000) for _ in channels)
    payload += struct.pack(">HHh", 1, 0, 50)
    return framed(0x32, 1001, payload)


class C37V2Tests(unittest.TestCase):
    """Ensure CFG-2 controls data offsets/scaling and malformed frames fail."""

    def test_decodes_cfg2_and_data(self) -> None:
        configuration = parse_cfg2(cfg2(), 1001)
        payload = struct.pack(">H", 0)
        for magnitude in range(1, 7):
            payload += struct.pack(">hh", magnitude * 10, 0)
        payload += struct.pack(">hh", 25, -2)
        decoded = parse_data(framed(0x02, 1001, payload), configuration)
        self.assertEqual(decoded.phasors["VA"], 10.0)
        self.assertEqual(decoded.phasors["IC"], 60.0)
        self.assertEqual(decoded.frequency, 50.025)
        self.assertEqual(decoded.rocof, -0.02)

    def test_rejects_bad_crc(self) -> None:
        frame = bytearray(cfg2())
        frame[-1] ^= 1
        with self.assertRaisesRegex(C37V2Error, "CRC"):
            parse_cfg2(bytes(frame), 1001)


if __name__ == "__main__":
    unittest.main()
