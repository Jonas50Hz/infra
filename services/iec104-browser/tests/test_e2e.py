"""Pure validation tests for the IEC 104 browser WebSocket probe."""

from __future__ import annotations

import unittest

from iec104_browser.e2e import ProbeError, _matching_fixture_messages, validate_messages


class BrowserProbeTests(unittest.TestCase):
    """Require the complete typed fixture evidence from a browser stream."""

    def test_accepts_all_fixture_values(self) -> None:
        validate_messages(_messages())

    def test_rejects_missing_or_invalid_fixture_value(self) -> None:
        with self.assertRaisesRegex(ProbeError, "did not receive M_ME_NC_1"):
            validate_messages(_messages()[:2])
        messages = _messages()
        messages[2]["value"] = 49.9
        with self.assertRaisesRegex(ProbeError, "short-float"):
            validate_messages(messages)

    def test_selects_expected_fixture_values_after_stale_records(self) -> None:
        expected = _messages()
        stale = _messages()
        stale[2]["value"] = 49.9

        selected = _matching_fixture_messages(expected + stale)

        self.assertIsNotNone(selected)
        validate_messages(selected or [])


def _messages() -> list[dict[str, object]]:
    return [
        {
            "type_id": "M_SP_NA_1",
            "cause_code": 3,
            "cause_name": "SPONTANEOUS",
            "common_address": 1,
            "information_object_address": 10,
            "quality_value": 32,
            "quality_flags": ["substituted"],
            "value": True,
        },
        {
            "type_id": "M_DP_NA_1",
            "cause_code": 3,
            "cause_name": "SPONTANEOUS",
            "common_address": 1,
            "information_object_address": 11,
            "quality_value": 16,
            "quality_flags": ["blocked"],
            "value": 2,
        },
        {
            "type_id": "M_ME_NC_1",
            "cause_code": 3,
            "cause_name": "SPONTANEOUS",
            "common_address": 1,
            "information_object_address": 12,
            "quality_value": 1,
            "quality_flags": ["overflow"],
            "value": 50.01,
        },
    ]


if __name__ == "__main__":
    unittest.main()