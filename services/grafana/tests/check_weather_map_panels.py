"""Assert both Berlin weather-map geomap panels return rows a marker layer can plot.

Each provisioned panel target is sent to Grafana's /api/ds/query exactly as the
dashboard defines it, so a wrong datasource target shape or a Druid SQL error
fails here rather than silently rendering an empty map.
"""

from __future__ import annotations

import base64
import json
import os
import sys
import time
from typing import Any
import urllib.error
import urllib.request

GRAFANA_ORIGIN = os.environ.get("GRAFANA_ORIGIN", "http://localhost:3001")
GRAFANA_AUTH = os.environ.get("GRAFANA_AUTH", "wama-admin:wama-admin")
DASHBOARD_UID = os.environ.get("DASHBOARD_UID", "wama-weather-map-berlin")
REQUEST_TIMEOUT_SECONDS = 90.0
EXPECTED_ROWS = {
    "PMU frequency heat map": 5,
    "Open-Meteo wind and weather": 3,
}


def request_json(path: str, payload: dict | None = None) -> tuple[int, Any]:
    """Return the status and decoded body for one authenticated Grafana call."""

    authorization = base64.b64encode(GRAFANA_AUTH.encode()).decode()
    request = urllib.request.Request(
        f"{GRAFANA_ORIGIN}{path}",
        data=None if payload is None else json.dumps(payload).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {authorization}",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()[:600]


def interpolated(target: dict, from_ms: int, to_ms: int) -> dict:
    """Substitute the dashboard time macros Grafana expands in the browser."""

    target = dict(target)
    builder = target.get("builder")
    if isinstance(builder, dict) and "query" in builder:
        target["builder"] = dict(builder)
        target["builder"]["query"] = (
            builder["query"].replace("${__from}", str(from_ms)).replace("${__to}", str(to_ms))
        )
    return target


def main() -> int:
    status, dashboard = request_json(f"/api/dashboards/uid/{DASHBOARD_UID}")
    if status != 200:
        print(f"FAIL: dashboard {DASHBOARD_UID} is not provisioned: {status} {dashboard}")
        return 1

    panels = dashboard["dashboard"]["panels"]
    to_ms = int(time.time() * 1_000)
    from_ms = to_ms - 6 * 3_600 * 1_000
    failures: list[str] = []

    for panel in panels:
        title = panel["title"]
        target = interpolated(panel["targets"][0], from_ms, to_ms)
        target["datasource"] = panel["datasource"]
        reference = target.get("refId", "A")

        status, result = request_json(
            "/api/ds/query",
            {"queries": [target], "from": str(from_ms), "to": str(to_ms)},
        )
        if status != 200:
            failures.append(f"{title}: Grafana returned HTTP {status}: {result}")
            continue

        frames = result["results"][reference].get("frames", [])
        fields: list[str] = []
        rows = 0
        for frame in frames:
            fields = [field["name"] for field in frame["schema"]["fields"]]
            values = frame["data"]["values"]
            rows += len(values[0]) if values else 0

        expected_rows = EXPECTED_ROWS.get(title)
        if expected_rows is not None and rows != expected_rows:
            failures.append(f"{title}: expected {expected_rows} mapped rows, received {rows}")

        basemap = panel["options"]["basemap"]["type"]
        if basemap != "osm-standard":
            failures.append(f"{title}: basemap is {basemap!r}, expected keyless 'osm-standard'")

        layer = panel["options"]["layers"][0]
        location = layer["config"]["location"]
        required = {location["latitude"], location["longitude"]}
        style = layer["config"]["style"]
        required.update(
            style[dimension]["field"]
            for dimension in ("color", "size")
            if isinstance(style.get(dimension), dict) and style[dimension].get("field")
        )
        missing = sorted(name for name in required if name not in fields)
        if missing:
            failures.append(f"{title}: frame is missing layer fields {missing}; has {fields}")

        print(f"{title}: {rows} rows, fields {fields}")

    if failures:
        print("\nFAILURES:")
        for failure in failures:
            print(f" - {failure}")
        return 1

    print("\nBoth geomap panels returned mappable rows for every layer field.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
