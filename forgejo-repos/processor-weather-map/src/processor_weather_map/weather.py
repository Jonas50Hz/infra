"""Open-Meteo collection and Common Format weather measurement serialization."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from math import isfinite
from numbers import Real
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from wama_processor.generated.rtd_schema_pb2 import MCCSMeasurementValue

BERLIN_LATITUDE = 52.5200
BERLIN_LONGITUDE = 13.4050
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
REQUEST_TIMEOUT_SECONDS = 20.0
WEATHER_MRIDS = {
    "temperature_c": "urn:wama:poc:weather:berlin:temperature-2m-c",
    "wind_speed_kmh": "urn:wama:poc:weather:berlin:wind-speed-10m-kmh",
    "wind_direction_degrees": "urn:wama:poc:weather:berlin:wind-direction-10m-degrees",
}


class WeatherCollectionError(RuntimeError):
    """Raised when Open-Meteo cannot provide a complete numeric weather reading."""


@dataclass(frozen=True)
class WeatherReading:
    """The current Berlin weather fields required by the map."""

    temperature_c: float
    wind_speed_kmh: float
    wind_direction_degrees: float


Request = Callable[[str, dict[str, str], float], dict[str, object]]


class WeatherCollector:
    """Fetch the one Open-Meteo current-weather request used by this processor."""

    def __init__(self, request: Request | None = None) -> None:
        self._request = request or _open_meteo_request

    def collect(self) -> WeatherReading:
        """Return complete finite numeric fields or a descriptive collection error."""

        try:
            payload = self._request(
                OPEN_METEO_URL,
                {
                    "latitude": str(BERLIN_LATITUDE),
                    "longitude": str(BERLIN_LONGITUDE),
                    "current": "temperature_2m,wind_speed_10m,wind_direction_10m",
                },
                REQUEST_TIMEOUT_SECONDS,
            )
        except (HTTPError, URLError, OSError, TimeoutError, json.JSONDecodeError) as error:
            raise WeatherCollectionError(f"Open-Meteo request failed: {error}") from error
        if not isinstance(payload, dict):
            raise WeatherCollectionError("Open-Meteo response must be an object")
        current = payload.get("current")
        if not isinstance(current, dict):
            raise WeatherCollectionError("Open-Meteo response lacks current weather")
        try:
            return WeatherReading(
                temperature_c=_finite_number(current, "temperature_2m"),
                wind_speed_kmh=_finite_number(current, "wind_speed_10m"),
                wind_direction_degrees=_finite_number(current, "wind_direction_10m"),
            )
        except ValueError as error:
            raise WeatherCollectionError(str(error)) from error


def weather_measurements(
    *,
    temperature_c: float,
    wind_speed_kmh: float,
    wind_direction_degrees: float,
    observed_at: datetime | None = None,
) -> dict[str, MCCSMeasurementValue]:
    """Serialize raw valid Common Format records for the LiveMeasurement topic."""

    values = {
        "temperature_c": temperature_c,
        "wind_speed_kmh": wind_speed_kmh,
        "wind_direction_degrees": wind_direction_degrees,
    }
    timestamp = observed_at or datetime.now(timezone.utc)
    if timestamp.tzinfo is None:
        raise ValueError("observed_at must be timezone-aware")
    seconds = int(timestamp.timestamp())
    records: dict[str, MCCSMeasurementValue] = {}
    for name, value in values.items():
        numeric_value = _finite_number(values, name)
        record = MCCSMeasurementValue(mrid=WEATHER_MRIDS[name], double_value=numeric_value)
        record.timestamp_mccs.seconds = seconds
        record.timestamp_mccs.nanos = timestamp.microsecond * 1_000
        record.quality.valid = True
        records[name] = record
    return records


def _finite_number(values: dict[str, Any], name: str) -> float:
    value = values.get(name)
    if isinstance(value, bool) or not isinstance(value, Real) or not isfinite(float(value)):
        raise ValueError(f"Open-Meteo current.{name} must be a finite numeric value")
    return float(value)


def _open_meteo_request(url: str, params: dict[str, str], timeout: float) -> dict[str, object]:
    request_url = f"{url}?{urlencode(params)}"
    with urlopen(request_url, timeout=timeout) as response:  # noqa: S310 -- fixed HTTPS endpoint
        payload = json.load(response)
    if not isinstance(payload, dict):
        raise WeatherCollectionError("Open-Meteo response must be an object")
    return payload
