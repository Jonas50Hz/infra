"""Timestamp validation tests for the C37.118 gateway runtime."""

from __future__ import annotations

import unittest

from gateway_c37_118.c37_v2 import C37V2Error
from gateway_c37_118.gateway_runtime import _timestamp


class GatewayRuntimeTests(unittest.TestCase):
    """Accept a short reporting-boundary lead while rejecting clock skew."""

    def test_accepts_timestamp_within_future_scheduling_tolerance(self) -> None:
        timestamp = _timestamp(100, 200_000, 1_000_000, now=100.0)

        self.assertEqual((timestamp.seconds, timestamp.nanos), (100, 200_000_000))

    def test_rejects_timestamp_beyond_future_scheduling_tolerance(self) -> None:
        with self.assertRaisesRegex(C37V2Error, "future tolerance"):
            _timestamp(101, 0, 1_000_000, now=100.0)


if __name__ == "__main__":
    unittest.main()
