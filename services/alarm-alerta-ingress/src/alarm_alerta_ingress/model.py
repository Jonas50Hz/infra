"""WAMA Alarm and Alerta representations used by reconciliation."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


WAMA_ENVIRONMENT = "WAMA"
WAMA_CUSTOMER = "wama"
WAMA_MANAGED_TAG = "wama-managed"
WAMA_MANAGED_BY_ATTRIBUTE = "wama_managed_by"
WAMA_MANAGED_BY_VALUE = "alarm-alerta-ingress"
WAMA_ALARM_KEY_ATTRIBUTE = "wama_alarm_key"
WAMA_EPISODE_ID_ATTRIBUTE = "wama_episode_id"


@dataclass(frozen=True)
class AlarmIdentity:
    """The stable source identity encoded in an Alarm Kafka key."""

    alarm_key: str
    mrid: str
    rule_id: str


@dataclass(frozen=True)
class DesiredAlarm:
    """One validated active desired state from the compacted Alarm topic."""

    activated_at: datetime
    episode_id: str
    evidence: tuple[tuple[str, str], ...]
    identity: AlarmIdentity
    observed_at: datetime
    rule_revision: str
    severity: str
    summary: str

    @property
    def event(self) -> str:
        """Return the immutable Alerta event identity for this activation."""

        return f"wama-alarm/{self.episode_id}"

    def alerta_payload(self) -> dict[str, Any]:
        """Map desired state without changing Alerta's native severity."""

        attributes = {
            WAMA_MANAGED_BY_ATTRIBUTE: WAMA_MANAGED_BY_VALUE,
            WAMA_ALARM_KEY_ATTRIBUTE: self.identity.alarm_key,
            WAMA_EPISODE_ID_ATTRIBUTE: self.episode_id,
            "wama_activated_at": _timestamp_text(self.activated_at),
            "wama_observed_at": _timestamp_text(self.observed_at),
            "wama_rule_id": self.identity.rule_id,
            "wama_rule_revision": self.rule_revision,
            "wama_severity": self.severity,
        }
        for name, value in self.evidence:
            attributes[f"wama_evidence_{name}"] = value
        return {
            "attributes": attributes,
            "customer": WAMA_CUSTOMER,
            "environment": WAMA_ENVIRONMENT,
            "event": self.event,
            "group": "WAMA",
            "origin": WAMA_MANAGED_BY_VALUE,
            "resource": self.identity.mrid,
            "service": ["wama-alarm"],
            "severity": "indeterminate",
            "tags": ["wama", WAMA_MANAGED_TAG, "wama-alarm"],
            "text": f"[WAMA {self.severity}] {self.summary}",
            "value": f"WAMA {self.severity}",
        }


@dataclass(frozen=True)
class ManagedRemoteAlert:
    """A currently actionable, ingress-owned Alerta alert."""

    alert_id: str
    alarm_key: str
    event: str
    resource: str
    status: str


def _timestamp_text(value: datetime) -> str:
    normalized = value.astimezone(timezone.utc)
    return normalized.isoformat(timespec="milliseconds").replace("+00:00", "Z")