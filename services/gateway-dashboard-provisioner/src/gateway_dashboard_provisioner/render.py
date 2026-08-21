"""Deterministic Grafana JSON rendering for active gateway sources."""

from __future__ import annotations

from collections.abc import Iterable
from hashlib import sha256
from math import ceil
from typing import Any, Final

from gateway_dashboard_provisioner.model import GatewaySignal, GatewaySource


DRUID_DATASOURCE_UID: Final = "druid"
DRUID_DATASOURCE_TYPE: Final = "grafadruid-druid-datasource"
LIVE_MEASUREMENTS_DATASOURCE: Final = "live_measurements"
FLEET_DASHBOARD_UID: Final = "wama-gateway-fleet"

_QUANTITY_DETAILS: Final = {
    "voltage": ("Phase Voltages", "volt"),
    "current": ("Phase Currents", "amp"),
    "frequency": ("Frequency", "hertz"),
    "rocof": ("ROCOF (Hz/s)", "suffix:Hz/s"),
}
_QUANTITY_ORDER: Final = ("voltage", "current", "frequency", "rocof")


class DashboardRenderError(ValueError):
    """Raised when validated source state cannot form a safe dashboard."""


def dashboard_uid(source_id: str) -> str:
    """Return a stable Grafana UID without exposing source text as an identifier."""

    digest = sha256(source_id.encode("utf-8")).hexdigest()[:24]
    return f"wama-gateway-{digest}"


def source_dashboard_filename(source_id: str) -> str:
    """Return a deterministic filesystem-safe name for one source dashboard."""

    digest = sha256(source_id.encode("utf-8")).hexdigest()[:24]
    return f"source-{digest}.json"


def render_snapshot(sources: Iterable[GatewaySource]) -> dict[str, dict[str, Any]]:
    """Render the fleet view and all active source pages in stable source order."""

    ordered_sources = tuple(sorted(sources, key=lambda source: source.source_id))
    return {
        "fleet.json": render_fleet_dashboard(ordered_sources),
        **{
            source_dashboard_filename(source.source_id): render_gateway_dashboard(source)
            for source in ordered_sources
        },
    }


def render_fleet_dashboard(sources: Iterable[GatewaySource]) -> dict[str, Any]:
    """Render the permanent operator entry point for active gateway pages."""

    ordered_sources = tuple(sorted(sources, key=lambda source: source.source_id))
    return {
        "annotations": {"list": []},
        "editable": False,
        "links": [
            {
                "includeTimeRange": True,
                "keepTime": True,
                "targetBlank": False,
                "title": f"{source.display_name} ({source.source_id})",
                "type": "link",
                "url": f"/d/{dashboard_uid(source.source_id)}",
            }
            for source in ordered_sources
        ],
        "panels": [
            {
                "gridPos": {"h": 12, "w": 24, "x": 0, "y": 0},
                "id": 1,
                "options": {
                    "content": _fleet_content(ordered_sources),
                    "mode": "markdown",
                },
                "title": "Active Gateway Sources",
                "transparent": True,
                "type": "text",
            }
        ],
        "refresh": "30s",
        "schemaVersion": 41,
        "tags": ["wama", "gateway", "masterdata"],
        "time": {"from": "now-15m", "to": "now"},
        "timezone": "browser",
        "title": "WAMA Gateway Fleet",
        "uid": FLEET_DASHBOARD_UID,
        "version": 1,
        "weekStart": "",
    }


def render_gateway_dashboard(source: GatewaySource) -> dict[str, Any]:
    """Render one active source page using only its approved signal mappings."""

    panels: list[dict[str, Any]] = [_metadata_panel(source)]
    quantity_groups = _quantity_groups(source.signals)
    for index, (quantity, signals) in enumerate(quantity_groups):
        title, unit = _QUANTITY_DETAILS[quantity]
        panels.append(
            _series_panel(
                panel_id=10 + index,
                title=title,
                unit=unit,
                signals=signals,
                x=(index % 2) * 12,
                y=6 + (index // 2) * 8,
            )
        )

    data_row = 6 + ceil(len(quantity_groups) / 2) * 8
    panels.extend(
        (
            _freshness_panel(source.signals, data_row),
            _latest_records_panel(source.signals, data_row),
        )
    )
    return {
        "annotations": {"list": []},
        "editable": False,
        "graphTooltip": 1,
        "links": [
            {
                "includeTimeRange": True,
                "keepTime": True,
                "targetBlank": False,
                "title": "Gateway Fleet",
                "type": "link",
                "url": f"/d/{FLEET_DASHBOARD_UID}",
            }
        ],
        "panels": panels,
        "refresh": "5s",
        "schemaVersion": 41,
        "tags": ["wama", "gateway", source.source_id],
        "time": {"from": "now-15m", "to": "now"},
        "timezone": "browser",
        "title": f"WAMA Gateway: {source.display_name}",
        "uid": dashboard_uid(source.source_id),
        "version": 1,
        "weekStart": "",
    }


def _fleet_content(sources: tuple[GatewaySource, ...]) -> str:
    if not sources:
        return "No active gateway sources are currently projected in Masterdata."
    lines = ["## Active gateway sources", ""]
    for source in sources:
        lines.append(
            f"- [{_markdown_text(source.display_name)} ({_markdown_code(source.source_id)})]"
            f"(/d/{dashboard_uid(source.source_id)})"
        )
    return "\n".join(lines)


def _metadata_panel(source: GatewaySource) -> dict[str, Any]:
    return {
        "gridPos": {"h": 6, "w": 24, "x": 0, "y": 0},
        "id": 1,
        "options": {
            "content": "\n".join(
                (
                    f"**Source:** `{_markdown_code(source.source_id)}`",
                    f"**Site:** {_markdown_text(source.display_name)} "
                    f"(`{_markdown_code(source.site_id)}`)",
                    f"**C37.118 endpoint:** `{_markdown_code(source.ip_address)}:{source.port}`",
                    f"**PMU IDCODE:** `{source.pmu_idcode}`",
                    f"**Catalog:** `{_markdown_code(source.catalog_id)}` "
                    f"revision `{_markdown_code(source.catalog_revision)}`",
                    f"**Published:** `{source.published_at.isoformat()}`",
                )
            ),
            "mode": "markdown",
        },
        "title": "Gateway Metadata",
        "transparent": True,
        "type": "text",
    }


def _quantity_groups(
    signals: tuple[GatewaySignal, ...],
) -> tuple[tuple[str, tuple[GatewaySignal, ...]], ...]:
    grouped = {
        quantity: tuple(signal for signal in signals if signal.quantity == quantity)
        for quantity in _QUANTITY_ORDER
    }
    unknown_quantities = sorted(
        {signal.quantity for signal in signals}.difference(_QUANTITY_DETAILS)
    )
    if unknown_quantities:
        names = ", ".join(unknown_quantities)
        raise DashboardRenderError(f"Unsupported gateway signal quantities: {names}")
    return tuple((quantity, signals) for quantity, signals in grouped.items() if signals)


def _series_panel(
    panel_id: int,
    title: str,
    unit: str,
    signals: tuple[GatewaySignal, ...],
    x: int,
    y: int,
) -> dict[str, Any]:
    return {
        "datasource": _druid_datasource(),
        "fieldConfig": {
            "defaults": {"displayName": "${__field.labels.signal}", "unit": unit},
            "overrides": [],
        },
        "gridPos": {"h": 8, "w": 12, "x": x, "y": y},
        "id": panel_id,
        "options": {
            "legend": {
                "calcs": ["lastNotNull", "min", "max"],
                "displayMode": "table",
                "placement": "bottom",
                "showLegend": True,
            },
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
        "targets": [_druid_target(_series_query(signals), "wide")],
        "title": title,
        "type": "timeseries",
    }


def _freshness_panel(signals: tuple[GatewaySignal, ...], y: int) -> dict[str, Any]:
    return {
        "datasource": _druid_datasource(),
        "fieldConfig": {
            "defaults": {"noValue": "No measurements", "unit": "dateTimeAsIso"},
            "overrides": [],
        },
        "gridPos": {"h": 8, "w": 8, "x": 0, "y": y},
        "id": 100,
        "options": {
            "colorMode": "value",
            "graphMode": "none",
            "justifyMode": "center",
            "orientation": "auto",
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "textMode": "value_and_name",
        },
        "targets": [_druid_target(_freshness_query(signals), "table")],
        "title": "Last Measurement",
        "type": "stat",
    }


def _latest_records_panel(signals: tuple[GatewaySignal, ...], y: int) -> dict[str, Any]:
    return {
        "datasource": _druid_datasource(),
        "fieldConfig": {
            "defaults": {
                "custom": {
                    "align": "auto",
                    "cellOptions": {"type": "auto"},
                    "inspect": False,
                }
            },
            "overrides": [],
        },
        "gridPos": {"h": 8, "w": 16, "x": 8, "y": y},
        "id": 101,
        "options": {"cellHeight": "sm", "showHeader": True},
        "targets": [_druid_target(_latest_records_query(signals), "long")],
        "title": "Latest Valid Records",
        "type": "table",
    }


def _druid_datasource() -> dict[str, str]:
    return {"type": DRUID_DATASOURCE_TYPE, "uid": DRUID_DATASOURCE_UID}


def _druid_target(query: str, output_format: str) -> dict[str, Any]:
    return {
        "builder": {"query": query, "queryType": "sql"},
        "refId": "A",
        "settings": {"contextParameters": [], "format": output_format},
    }


def _series_query(signals: tuple[GatewaySignal, ...]) -> str:
    aliases = " ".join(
        f"WHEN {_sql_literal(signal.mrid)} THEN {_sql_literal(signal.signal_id)}"
        for signal in signals
    )
    return (
        'SELECT "__time", CASE "mrid" '
        f'{aliases} END AS "signal", "double_value" '
        f'FROM "{LIVE_MEASUREMENTS_DATASOURCE}" '
        f'WHERE "mrid" IN ({_mrid_list(signals)}) '
        "AND \"quality_valid\" = 'true' "
        'AND "double_value" IS NOT NULL '
        'AND "__time" >= MILLIS_TO_TIMESTAMP(${__from}) '
        'AND "__time" <= MILLIS_TO_TIMESTAMP(${__to}) '
        'ORDER BY "__time" ASC'
    )


def _freshness_query(signals: tuple[GatewaySignal, ...]) -> str:
    return (
        'SELECT MAX("__time") AS "last_measurement" '
        f'FROM "{LIVE_MEASUREMENTS_DATASOURCE}" '
        f'WHERE "mrid" IN ({_mrid_list(signals)}) '
        "AND \"quality_valid\" = 'true' "
        'AND "double_value" IS NOT NULL'
    )


def _latest_records_query(signals: tuple[GatewaySignal, ...]) -> str:
    return (
        'SELECT "__time", "mrid", "double_value", "timestamp_field", '
        '"timestamp_gateway", "timestamp_mccs" '
        f'FROM "{LIVE_MEASUREMENTS_DATASOURCE}" '
        f'WHERE "mrid" IN ({_mrid_list(signals)}) '
        "AND \"quality_valid\" = 'true' "
        'AND "double_value" IS NOT NULL '
        'AND "__time" >= MILLIS_TO_TIMESTAMP(${__from}) '
        'AND "__time" <= MILLIS_TO_TIMESTAMP(${__to}) '
        'ORDER BY "__time" DESC LIMIT 32'
    )


def _mrid_list(signals: tuple[GatewaySignal, ...]) -> str:
    return ", ".join(_sql_literal(signal.mrid) for signal in signals)


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _markdown_text(value: str) -> str:
    return value.replace("\\", "\\\\").replace("`", "\\`")


def _markdown_code(value: str) -> str:
    return value.replace("\\", "\\\\").replace("`", "\\`")