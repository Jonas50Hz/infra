"""Tests for deterministic Druid SQL and Common Format row reconstruction."""

from __future__ import annotations

from datetime import datetime, timezone
import unittest

from google.protobuf.timestamp_pb2 import Timestamp

from measurement_session_processor.druid import (
    DruidQueryError,
    MeasurementRow,
    _order_timestamp_ties,
    query_for_request,
)
from measurement_session_common.generated.measurement_session_pb2 import MeasurementSessionRequest


class DruidQueryTests(unittest.TestCase):
    """Prove user MRIDs cannot alter the generated query shape."""

    def test_escapes_mrid_literals_and_uses_half_open_bounds(self) -> None:
        request = _request("urn:wama:poc:bay''-01")

        query = query_for_request(request, "live_measurements")

        self.assertIn("urn:wama:poc:bay''''-01", query)
        self.assertIn('"__time" < MILLIS_TO_TIMESTAMP(', query)
        self.assertIn('ORDER BY "__time" ASC', query)
        self.assertNotIn('ORDER BY "__time" ASC, "mrid" ASC', query)

    def test_stabilizes_mrid_order_within_one_timestamp(self) -> None:
        rows = _order_timestamp_ties(
            (
                _row("urn:wama:poc:b"),
                _row("urn:wama:poc:a"),
            ),
            max_tied_rows=2,
        )

        self.assertEqual([row.mrid for row in rows], ["urn:wama:poc:a", "urn:wama:poc:b"])

    def test_rejects_timestamp_regression(self) -> None:
        with self.assertRaisesRegex(DruidQueryError, "not ordered"):
            tuple(
                _order_timestamp_ties(
                    (
                        _row("urn:wama:poc:a", minute=1),
                        _row("urn:wama:poc:b", minute=0),
                    ),
                    max_tied_rows=2,
                )
            )

    def test_reconstructs_boolean_common_format_value(self) -> None:
        row = MeasurementRow.from_druid(
            {
                "timestamp_mccs": "2026-08-19T09:00:00Z",
                "mrid": "urn:wama:poc:breaker",
                "bool_value": "true",
                "quality_valid": "true",
            }
        )

        self.assertEqual(row.value_type, "bool")
        self.assertIs(row.bool_value, True)
        self.assertIs(row.quality_valid, True)


def _request(mrid: str) -> MeasurementSessionRequest:
    request = MeasurementSessionRequest(
        session_id="4ff0a4c6-1ae4-4f51-b1b7-d7762a7c4237",
        mrids=(mrid,),
    )
    for field_name, value in (
        ("requested_at", datetime(2026, 8, 19, 9, 2, tzinfo=timezone.utc)),
        ("started_at", datetime(2026, 8, 19, 9, 0, tzinfo=timezone.utc)),
        ("ended_at", datetime(2026, 8, 19, 9, 1, tzinfo=timezone.utc)),
    ):
        timestamp = Timestamp()
        timestamp.FromDatetime(value)
        getattr(request, field_name).CopyFrom(timestamp)
    return request


def _row(mrid: str, minute: int = 0) -> MeasurementRow:
    return MeasurementRow(
        timestamp_mccs=datetime(2026, 8, 19, 9, minute, tzinfo=timezone.utc),
        mrid=mrid,
        value_type="double",
        double_value=50.01,
    )