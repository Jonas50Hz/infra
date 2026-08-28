"""Tests for the narrow, marker-scoped Alerta client."""

from __future__ import annotations

import unittest

from alarm_alerta_ingress.client import AlertaClient, AlertaClientError
from alarm_alerta_ingress.codec import decode_alarm
from alarm_alerta_ingress.config import Settings
from alarm_alerta_ingress.consumer import AlarmIngressWorker
from alarm_alerta_ingress.model import (
    WAMA_ALARM_KEY_ATTRIBUTE,
    WAMA_MANAGED_BY_ATTRIBUTE,
    WAMA_MANAGED_BY_VALUE,
    WAMA_MANAGED_TAG,
)
from alarm_alerta_ingress.state import AlarmRegistry
from test_codec import _alarm_record


class AlertaClientTests(unittest.TestCase):
    """Ensure ingress actions stay within the explicit WAMA marker boundary."""

    def test_lists_only_marked_open_or_ack_alerts(self) -> None:
        session = _Session(
            [
                _Response(
                    {
                        "status": "ok",
                        "more": False,
                        "page": 1,
                        "pages": 1,
                        "alerts": [
                            _remote_alert("managed-open", "open"),
                            _remote_alert("managed-ack", "ack"),
                            _remote_alert("managed-closed", "closed"),
                            {"id": "foreign", "status": "open", "environment": "WAMA"},
                        ],
                    }
                )
            ]
        )
        client = AlertaClient("http://alerta:8080", "test-key", 10, session=session)

        alerts = client.list_managed_active_or_ack()

        self.assertEqual([alert.alert_id for alert in alerts], ["managed-ack", "managed-open"])
        self.assertEqual(session.requests[0][0:2], ("GET", "http://alerta:8080/api/alerts"))

    def test_reconciles_a_managed_alert_from_a_later_page(self) -> None:
        session = _Session(
            [
                _Response(
                    {
                        "status": "ok",
                        "more": True,
                        "page": 1,
                        "pages": 2,
                        "alerts": [
                            {
                                "id": "foreign",
                                "status": "open",
                                "environment": "WAMA",
                            }
                        ],
                    }
                ),
                _Response(
                    {
                        "status": "ok",
                        "more": False,
                        "page": 2,
                        "pages": 2,
                        "alerts": [_remote_alert("managed-later-page", "open")],
                    }
                ),
                _Response({"status": "ok"}),
            ]
        )
        client = AlertaClient("http://alerta:8080", "test-key", 10, session=session)
        worker = AlarmIngressWorker(_settings(), client=client)

        worker.reconcile_remote_snapshot(AlarmRegistry())

        self.assertEqual(
            session.requests,
            [
                (
                    "GET",
                    "http://alerta:8080/api/alerts",
                    {
                        "headers": {"Authorization": "Key test-key"},
                        "params": {"environment": "WAMA", "page-size": "1000", "page": 1},
                        "timeout": 10,
                    },
                ),
                (
                    "GET",
                    "http://alerta:8080/api/alerts",
                    {
                        "headers": {"Authorization": "Key test-key"},
                        "params": {"environment": "WAMA", "page-size": "1000", "page": 2},
                        "timeout": 10,
                    },
                ),
                (
                    "PUT",
                    "http://alerta:8080/api/alert/managed-later-page/status",
                    {
                        "headers": {"Authorization": "Key test-key"},
                        "json": {
                            "status": "closed",
                            "text": "WAMA Alarm desired state cleared or superseded",
                        },
                        "timeout": 10,
                    },
                ),
            ],
        )

    def test_rejects_non_advancing_pagination(self) -> None:
        session = _Session(
            [
                _Response(
                    {
                        "status": "ok",
                        "more": True,
                        "page": 1,
                        "pages": 1,
                        "alerts": [],
                    }
                )
            ]
        )
        client = AlertaClient("http://alerta:8080", "test-key", 10, session=session)

        with self.assertRaisesRegex(AlertaClientError, "pagination"):
            client.list_managed_active_or_ack()

        self.assertEqual(len(session.requests), 1)

    def test_upserts_and_closes_with_native_status_route(self) -> None:
        key, payload = _alarm_record()
        alarm = decode_alarm(key, payload)
        session = _Session([_Response({"status": "ok"}), _Response({"status": "ok"})])
        client = AlertaClient("http://alerta:8080", "test-key", 10, session=session)

        client.upsert(alarm)
        client.close_alert("alert-1", "WAMA Alarm desired state cleared")

        self.assertEqual(session.requests[0][0:2], ("POST", "http://alerta:8080/api/alert"))
        self.assertEqual(session.requests[0][2]["headers"], {"Authorization": "Key test-key"})
        self.assertEqual(session.requests[0][2]["json"]["severity"], "indeterminate")
        self.assertEqual(
            session.requests[1],
            (
                "PUT",
                "http://alerta:8080/api/alert/alert-1/status",
                {
                    "headers": {"Authorization": "Key test-key"},
                    "json": {"status": "closed", "text": "WAMA Alarm desired state cleared"},
                    "timeout": 10,
                },
            ),
        )


def _remote_alert(alert_id: str, status: str) -> dict[str, object]:
    key, _ = _alarm_record()
    return {
        "attributes": {
            WAMA_ALARM_KEY_ATTRIBUTE: key.decode("utf-8"),
            WAMA_MANAGED_BY_ATTRIBUTE: WAMA_MANAGED_BY_VALUE,
        },
        "customer": "wama",
        "environment": "WAMA",
        "event": "wama-alarm/a0d5e631-962d-4ce3-86ba-04d4252a3285",
        "id": alert_id,
        "resource": "urn:wama:poc:pmu:bay-01:frequency",
        "status": status,
        "tags": [WAMA_MANAGED_TAG],
    }


def _settings() -> Settings:
    return Settings(
        alerta_api_key="test-key",
        alerta_request_timeout_seconds=10,
        alerta_url="http://alerta:8080",
        kafka_bootstrap_servers="kafka:9092",
        kafka_retry_interval_seconds=1,
        kafka_topic="Alarm",
        ready_file="/tmp/alarm-alerta-ingress-test-ready",
    )


class _Response:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def json(self) -> dict[str, object]:
        return self._payload

    def raise_for_status(self) -> None:
        return None


class _Session:
    def __init__(self, responses: list[_Response]) -> None:
        self._responses = responses
        self.requests: list[tuple[str, str, dict[str, object]]] = []

    def request(self, method: str, url: str, **kwargs: object) -> _Response:
        self.requests.append((method, url, kwargs))
        return self._responses.pop(0)