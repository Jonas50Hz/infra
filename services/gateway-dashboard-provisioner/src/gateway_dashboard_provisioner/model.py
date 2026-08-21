"""Validated gateway masterdata used to render Grafana dashboards."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class GatewaySignal:
    """One source-local signal with its immutable Common Format MRID."""

    signal_id: str
    source_channel: str
    mrid: str
    quantity: str
    unit: str


@dataclass(frozen=True)
class GatewaySource:
    """One active C37.118 source projected from compacted Masterdata."""

    source_id: str
    catalog_id: str
    catalog_revision: str
    published_at: datetime
    site_id: str
    display_name: str
    ip_address: str
    port: int
    pmu_idcode: int
    signals: tuple[GatewaySignal, ...]