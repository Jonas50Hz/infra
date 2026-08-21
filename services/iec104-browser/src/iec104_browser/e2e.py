"""One-shot WebSocket probe for the on-demand IEC 104 browser stream."""

from __future__ import annotations

import asyncio
import json
import math
import os
from typing import Any

import websockets


class ProbeError(RuntimeError):
    """Raised when the browser stream does not receive the expected IEC values."""


async def run() -> None:
    """Connect to the live stream and require all current fixture value types."""

    url = os.environ.get("IEC104_BROWSER_WS_URL", "ws://127.0.0.1:8080/v1/iec104/live")
    timeout_seconds = _positive_float(os.environ.get("IEC104_BROWSER_E2E_TIMEOUT_SECONDS", "30"))
    messages: list[dict[str, Any]] = []
    async with websockets.connect(url) as websocket:
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while asyncio.get_running_loop().time() < deadline:
            remaining = deadline - asyncio.get_running_loop().time()
            try:
                raw_message = await asyncio.wait_for(websocket.recv(), timeout=remaining)
            except TimeoutError as error:
                raise ProbeError(
                    "browser stream did not receive all fixture values: "
                    f"{messages[-12:]!r}"
                ) from error
            payload = _payload(raw_message)
            if payload.get("kind") == "message":
                messages.append(payload)
                fixture_messages = _matching_fixture_messages(messages)
                if fixture_messages is not None:
                    validate_messages(fixture_messages)
                    print("IEC 104 browser stream received all fixture values")
                    return
    raise ProbeError(
        "browser stream closed before receiving all fixture values: "
        f"{messages[-12:]!r}"
    )


def validate_messages(messages: list[dict[str, Any]]) -> None:
    """Require decoded wire evidence for the single, double, and float fixtures."""

    by_type = {message.get("type_id"): message for message in messages}
    for type_id in ("M_SP_NA_1", "M_DP_NA_1", "M_ME_NC_1"):
        message = by_type.get(type_id)
        if message is None:
            raise ProbeError(f"browser stream did not receive {type_id}")
        if message.get("cause_code") != 3 or message.get("cause_name") != "SPONTANEOUS":
            raise ProbeError(f"browser stream has an unexpected COT for {type_id}")
        if not isinstance(message.get("common_address"), int) or message["common_address"] <= 0:
            raise ProbeError(f"browser stream has an invalid common address for {type_id}")
        if not isinstance(message.get("information_object_address"), int) or message["information_object_address"] <= 0:
            raise ProbeError(f"browser stream has an invalid information-object address for {type_id}")
    if by_type["M_SP_NA_1"].get("value") is not True:
        raise ProbeError("browser stream has an unexpected single-point value")
    if by_type["M_DP_NA_1"].get("value") != 2:
        raise ProbeError("browser stream has an unexpected double-point value")
    short_float = by_type["M_ME_NC_1"].get("value")
    if not isinstance(short_float, (int, float)) or not math.isclose(short_float, 50.01, rel_tol=0, abs_tol=0.0001):
        raise ProbeError("browser stream has an unexpected short-float value")
    quality_by_type = {
        "M_SP_NA_1": (32, ["substituted"]),
        "M_DP_NA_1": (16, ["blocked"]),
        "M_ME_NC_1": (1, ["overflow"]),
    }
    for type_id, (quality_value, quality_flags) in quality_by_type.items():
        message = by_type[type_id]
        if message.get("quality_value") != quality_value or message.get("quality_flags") != quality_flags:
            raise ProbeError(f"browser stream has an unexpected quality descriptor for {type_id}")


def _has_fixture_values(messages: list[dict[str, Any]]) -> bool:
    return _matching_fixture_messages(messages) is not None


def _matching_fixture_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]] | None:
    """Return the expected fixture set without being distracted by stale records."""

    expected = (
        ("M_SP_NA_1", True, 32, ["substituted"]),
        ("M_DP_NA_1", 2, 16, ["blocked"]),
        ("M_ME_NC_1", 50.01, 1, ["overflow"]),
    )
    selected: list[dict[str, Any]] = []
    for type_id, expected_value, quality_value, quality_flags in expected:
        matching = next(
            (
                message
                for message in reversed(messages)
                if _matches_fixture_message(
                    message,
                    type_id,
                    expected_value,
                    quality_value,
                    quality_flags,
                )
            ),
            None,
        )
        if matching is None:
            return None
        selected.append(matching)
    return selected


def _matches_fixture_message(
    message: dict[str, Any],
    type_id: str,
    expected_value: bool | int | float,
    quality_value: int,
    quality_flags: list[str],
) -> bool:
    if (
        message.get("type_id") != type_id
        or message.get("cause_code") != 3
        or message.get("cause_name") != "SPONTANEOUS"
        or message.get("quality_value") != quality_value
        or message.get("quality_flags") != quality_flags
    ):
        return False
    value = message.get("value")
    if isinstance(expected_value, float):
        return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isclose(
            value,
            expected_value,
            rel_tol=0,
            abs_tol=0.0001,
        )
    return value == expected_value


def _payload(raw_message: str | bytes) -> dict[str, Any]:
    try:
        decoded = raw_message.decode("utf-8") if isinstance(raw_message, bytes) else raw_message
        payload = json.loads(decoded)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProbeError("browser stream returned malformed JSON") from error
    if not isinstance(payload, dict):
        raise ProbeError("browser stream returned a non-object JSON message")
    return payload


def _positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise ProbeError("IEC104_BROWSER_E2E_TIMEOUT_SECONDS must be a number") from error
    if parsed <= 0:
        raise ProbeError("IEC104_BROWSER_E2E_TIMEOUT_SECONDS must be greater than zero")
    return parsed


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except (OSError, ProbeError, websockets.WebSocketException) as error:
        raise SystemExit(f"IEC 104 browser probe failed: {error}") from error