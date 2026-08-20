"""Frame filtering tests for the output-only IEC 104 ingress guard."""

from __future__ import annotations

import unittest

from iec104_exporter.ingress import _allowed_transport_frames


class IngressFrameTests(unittest.TestCase):
    """Permit U/S transport frames and block I-frame application payloads."""

    def test_forwards_start_data_transfer_and_retains_incomplete_tail(self) -> None:
        start_data_transfer = b"\x68\x04\x07\x00\x00\x00"
        frames, pending, rejected = _allowed_transport_frames(start_data_transfer + b"\x68")

        self.assertEqual(frames, (start_data_transfer,))
        self.assertEqual(pending, b"\x68")
        self.assertFalse(rejected)

    def test_forwards_a_valid_supervisory_frame(self) -> None:
        supervisory = b"\x68\x04\x01\x00\x02\x00"

        frames, pending, rejected = _allowed_transport_frames(supervisory)

        self.assertEqual(frames, (supervisory,))
        self.assertEqual(pending, b"")
        self.assertFalse(rejected)

    def test_rejects_an_application_i_frame_without_forwarding_it(self) -> None:
        start_data_transfer = b"\x68\x04\x07\x00\x00\x00"
        interrogation = b"\x68\x0e\x00\x00\x00\x00" + b"\x00" * 10

        frames, pending, rejected = _allowed_transport_frames(start_data_transfer + interrogation)

        self.assertEqual(frames, (start_data_transfer,))
        self.assertEqual(pending, b"")
        self.assertTrue(rejected)

    def test_rejects_malformed_or_non_transport_control_frames(self) -> None:
        malformed_frames = (
            b"\x68\x04\x03\x00\x00\x00",  # Unsupported U-frame control field.
            b"\x68\x04\x07\x01\x00\x00",  # U frame with nonzero reserved byte.
            b"\x68\x04\x01\x01\x00\x00",  # S frame with invalid reserved byte.
            b"\x68\x06\x01\x00\x00\x00\x00\x00",  # S frame with an ASDU payload.
        )

        for frame in malformed_frames:
            with self.subTest(frame=frame.hex()):
                frames, pending, rejected = _allowed_transport_frames(frame)

                self.assertEqual(frames, ())
                self.assertEqual(pending, b"")
                self.assertTrue(rejected)


if __name__ == "__main__":
    unittest.main()