"""Shared raw-Protobuf fixtures for alarm threshold unit tests."""

from __future__ import annotations

from datetime import datetime, timezone

from processor_alarm_threshold.config import AlarmRule, SignalSelector
from processor_alarm_threshold.generated import masterdata_pb2, rtd_schema_pb2


CATALOG_ID = "c37-118-poc-v1"
FREQUENCY_MRID = "urn:wama:poc:pmu:bay-01:frequency"


def reviewed_rules() -> tuple[AlarmRule, ...]:
    """Return the initial reviewed threshold rule without filesystem setup."""

    return (
        AlarmRule(
            rule_id="frequency-over-50-1-hz",
            revision="1",
            selector=SignalSelector(
                mrid_glob="urn:wama:poc:pmu:*:frequency",
                value_kind="double",
                quantity="frequency",
                unit="Hz",
            ),
            operator="gt",
            threshold=50.1,
            severity="warning",
        ),
    )


def source(
    source_id: str,
    *,
    catalog_id: str = CATALOG_ID,
    mrids: tuple[str, ...] = (FREQUENCY_MRID,),
    value_kind: int = masterdata_pb2.MCCS_VALUE_KIND_DOUBLE,
    quantity: str = "frequency",
    unit: str = "Hz",
) -> bytes:
    """Build a valid-enough scoped source projection with named frequency signals."""

    message = masterdata_pb2.SourceMasterdata(
        source_id=source_id,
        catalog_id=catalog_id,
        catalog_revision="reviewed-revision",
    )
    message.published_at.FromDatetime(datetime(2026, 8, 26, tzinfo=timezone.utc))
    for index, mrid in enumerate(mrids, start=1):
        signal = message.signals.add()
        signal.signal_id = f"frequency-{index}"
        signal.source_channel = f"FREQ{index}"
        signal.mrid = mrid
        signal.value_kind = value_kind
        signal.quantity = quantity
        signal.unit = unit
    return message.SerializeToString()


def measurement(
    *,
    mrid: str = FREQUENCY_MRID,
    value: float = 50.2,
    observed_at: datetime,
    valid: bool | None = True,
    substituted: bool | None = None,
    operator_blocked: bool | None = None,
    overflow: bool | None = None,
    old_data: bool | None = None,
) -> bytes:
    """Build one Common Format floating-point measurement with explicit quality."""

    message = rtd_schema_pb2.MCCSMeasurementValue(mrid=mrid, double_value=value)
    message.timestamp_mccs.FromDatetime(observed_at)
    if valid is not None:
        message.quality.valid = valid
    for field, field_value in (
        ("substituted", substituted),
        ("operator_blocked", operator_blocked),
        ("overflow", overflow),
        ("old_data", old_data),
    ):
        if field_value is not None:
            setattr(message.quality, field, field_value)
    return message.SerializeToString()