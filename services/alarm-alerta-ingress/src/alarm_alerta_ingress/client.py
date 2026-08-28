"""Narrow Alerta v9.1 API client for ingress-owned alarm reconciliation."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import requests

from alarm_alerta_ingress.model import (
    WAMA_ALARM_KEY_ATTRIBUTE,
    WAMA_CUSTOMER,
    WAMA_ENVIRONMENT,
    WAMA_MANAGED_BY_ATTRIBUTE,
    WAMA_MANAGED_BY_VALUE,
    WAMA_MANAGED_TAG,
    DesiredAlarm,
    ManagedRemoteAlert,
)


class AlertaClientError(RuntimeError):
    """Raised when Alerta cannot provide a safe reconciliation response."""


class AlertaClient:
    """Call only the Alerta operations owned by the root Alarm ingress."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        request_timeout_seconds: int,
        session: requests.Session | None = None,
    ) -> None:
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._request_timeout_seconds = request_timeout_seconds
        self._session = requests.Session() if session is None else session

    def close_alert(self, alert_id: str, text: str) -> None:
        """Close an ingress-owned alert through Alerta's native status route."""

        response = self._request(
            "PUT",
            f"/api/alert/{alert_id}/status",
            json={"status": "closed", "text": text},
        )
        self._payload(response, "Alerta close")

    def list_managed_active_or_ack(self) -> tuple[ManagedRemoteAlert, ...]:
        """Return only active/ack alerts explicitly owned by this ingress."""

        managed_alerts: list[ManagedRemoteAlert] = []
        expected_pages: int | None = None
        page = 1
        while True:
            response = self._request(
                "GET",
                "/api/alerts",
                params={
                    "environment": WAMA_ENVIRONMENT,
                    "page-size": "1000",
                    "page": page,
                },
            )
            payload = self._payload(response, "Alerta alert list")
            raw_alerts = payload.get("alerts")
            if not isinstance(raw_alerts, list):
                raise AlertaClientError("Alerta alert list has no alerts array")
            response_page = _pagination_integer(payload, "page", minimum=1)
            response_pages = _pagination_integer(payload, "pages", minimum=0)
            more = payload.get("more")
            if not isinstance(more, bool):
                raise AlertaClientError("Alerta alert list pagination has no boolean more value")
            if response_page != page:
                raise AlertaClientError("Alerta alert list pagination returned an unexpected page")
            if expected_pages is None:
                expected_pages = response_pages
            elif response_pages != expected_pages:
                raise AlertaClientError("Alerta alert list pagination changed while reading pages")
            if response_pages == 0:
                if page != 1 or more or raw_alerts:
                    raise AlertaClientError("Alerta alert list pagination is inconsistent for an empty result")
                break
            if page > response_pages:
                raise AlertaClientError("Alerta alert list pagination returned a page beyond its limit")
            if more != (page < response_pages):
                raise AlertaClientError("Alerta alert list pagination more value is inconsistent")
            managed_alerts.extend(
                remote_alert
                for raw_alert in raw_alerts
                if (remote_alert := _managed_remote_alert(raw_alert)) is not None
            )
            if not more:
                break
            page += 1
        return tuple(sorted(managed_alerts, key=lambda alert: alert.alert_id))

    def upsert(self, alarm: DesiredAlarm) -> None:
        """Upsert one desired state through Alerta's native receive route."""

        response = self._request("POST", "/api/alert", json=alarm.alerta_payload())
        self._payload(response, "Alerta upsert")

    def _payload(self, response: requests.Response, operation: str) -> Mapping[str, Any]:
        try:
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, ValueError) as error:
            raise AlertaClientError(f"{operation} failed: {error}") from error
        if not isinstance(payload, Mapping):
            raise AlertaClientError(f"{operation} did not return an object")
        if payload.get("status") != "ok":
            raise AlertaClientError(f"{operation} did not return status=ok")
        return payload

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        headers = dict(kwargs.pop("headers", {}))
        headers["Authorization"] = f"Key {self._api_key}"
        try:
            return self._session.request(
                method,
                f"{self._base_url}{path}",
                headers=headers,
                timeout=self._request_timeout_seconds,
                **kwargs,
            )
        except requests.RequestException as error:
            raise AlertaClientError(f"Alerta request failed: {error}") from error


def _managed_remote_alert(raw_alert: object) -> ManagedRemoteAlert | None:
    if not isinstance(raw_alert, Mapping):
        return None
    tags = raw_alert.get("tags")
    attributes = raw_alert.get("attributes")
    if (
        raw_alert.get("status") not in {"open", "ack"}
        or raw_alert.get("environment") != WAMA_ENVIRONMENT
        or raw_alert.get("customer") != WAMA_CUSTOMER
        or not isinstance(tags, list)
        or WAMA_MANAGED_TAG not in tags
        or not isinstance(attributes, Mapping)
        or attributes.get(WAMA_MANAGED_BY_ATTRIBUTE) != WAMA_MANAGED_BY_VALUE
    ):
        return None
    alert_id = raw_alert.get("id")
    alarm_key = attributes.get(WAMA_ALARM_KEY_ATTRIBUTE)
    event = raw_alert.get("event")
    resource = raw_alert.get("resource")
    if not all(isinstance(value, str) and value for value in (alert_id, alarm_key, event, resource)):
        return None
    return ManagedRemoteAlert(
        alert_id=alert_id,
        alarm_key=alarm_key,
        event=event,
        resource=resource,
        status=raw_alert["status"],
    )


def _pagination_integer(payload: Mapping[str, Any], name: str, minimum: int) -> int:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise AlertaClientError(f"Alerta alert list pagination has an invalid {name} value")
    return value