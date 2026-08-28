"""Normalized IEC 104 monitor values delivered to live browser clients."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MonitorEvent:
    """One outbound monitor-direction ASDU observed by the control center."""

    cause_code: int
    cause_name: str
    common_address: int
    information_object_address: int
    quality_flags: tuple[str, ...]
    quality_value: int
    received_at: str
    type_id: str
    value: bool | int | float
    value_text: str

    def payload(self) -> dict[str, object]:
        """Return the JSON-safe event representation used by the WebSocket."""

        return {
            "kind": "message",
            "cause_code": self.cause_code,
            "cause_name": self.cause_name,
            "common_address": self.common_address,
            "information_object_address": self.information_object_address,
            "quality_flags": list(self.quality_flags),
            "quality_value": self.quality_value,
            "received_at": self.received_at,
            "type_id": self.type_id,
            "value": self.value,
            "value_text": self.value_text,
        }


@dataclass(frozen=True)
class MonitorStatus:
    """Current lifecycle state of the persistent IEC 104 control center."""

    active: bool
    state: str