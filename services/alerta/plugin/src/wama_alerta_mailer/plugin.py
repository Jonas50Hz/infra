"""Best-effort Mailpit notification for a newly opened WAMA alarm episode."""

from __future__ import annotations

from collections.abc import Mapping
import logging
from typing import Any

from alerta.plugins import PluginBase
from alerta.utils.mailer import Mailer
from flask import current_app


LOGGER = logging.getLogger(__name__)
WAMA_MANAGED_TAG = "wama-managed"
WAMA_MANAGED_BY_ATTRIBUTE = "wama_managed_by"
WAMA_MANAGED_BY_VALUE = "alarm-alerta-ingress"


class WamaInitialEpisodeMailer(PluginBase):
    """Notify the local test inbox only for the first active WAMA episode."""

    def pre_receive(self, alert: Any, **kwargs: Any) -> Any:
        """Leave the incoming alert unchanged before Alerta persists it."""

        return alert

    def post_receive(self, alert: Any, **kwargs: Any) -> Any:
        """Best-effort email only after Alerta creates a qualifying initial alert."""

        if not _is_first_wama_episode(alert):
            return alert
        recipient = self.get_config("WAMA_ALERT_RECIPIENT", "", **kwargs)
        if not isinstance(recipient, str) or not recipient.strip():
            LOGGER.error("WAMA alert recipient is not configured")
            return alert
        Mailer(current_app).send_email(
            recipient.strip(),
            _subject(alert),
            _body(alert),
        )
        return alert

    def status_change(self, alert: Any, status: str, text: str, **kwargs: Any) -> None:
        """Do not notify for acknowledgements, closures, or other status actions."""

        return None


def _is_first_wama_episode(alert: Any) -> bool:
    tags = getattr(alert, "tags", ())
    attributes = getattr(alert, "attributes", {})
    history = getattr(alert, "history", ())
    return (
        getattr(alert, "status", None) == "open"
        and getattr(alert, "repeat", None) is False
        and getattr(alert, "duplicate_count", None) == 0
        and getattr(alert, "previous_severity", None) == "indeterminate"
        and _only_initial_new_history(history)
        and isinstance(tags, (list, tuple, set))
        and WAMA_MANAGED_TAG in tags
        and isinstance(attributes, Mapping)
        and attributes.get(WAMA_MANAGED_BY_ATTRIBUTE) == WAMA_MANAGED_BY_VALUE
    )


def _only_initial_new_history(history: object) -> bool:
    if not isinstance(history, (list, tuple)) or len(history) != 1:
        return False
    initial_entry = history[0]
    if isinstance(initial_entry, Mapping):
        return initial_entry.get("type", initial_entry.get("change_type")) == "new"
    return getattr(initial_entry, "type", getattr(initial_entry, "change_type", None)) == "new"


def _subject(alert: Any) -> str:
    return f"[WAMA] {getattr(alert, 'resource', 'unknown')} {getattr(alert, 'event', 'unknown')}"


def _body(alert: Any) -> str:
    return "\n".join(
        (
            "A new WAMA alarm episode is open.",
            f"Resource: {getattr(alert, 'resource', '')}",
            f"Event: {getattr(alert, 'event', '')}",
            f"Severity: {getattr(alert, 'severity', '')}",
            f"Text: {getattr(alert, 'text', '')}",
        )
    )