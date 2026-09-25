"""Weather collection and Common Format serialization behavior."""

from __future__ import annotations

from datetime import datetime, timezone
import unittest

from processor_weather_map.weather import (
    BERLIN_LATITUDE,
    BERLIN_LONGITUDE,
    WEATHER_MRIDS,
    WeatherCollectionError,
    WeatherCollector,
    weather_measurements,
)


class WeatherCollectorTests(unittest.TestCase):
    def test_requests_berlin_current_weather_fields(self) -> None:
        requested: list[tuple[str, dict[str, str], float]] = []

        def request(url: str, params: dict[str, str], timeout: float) -> dict[str, object]:
            requested.append((url, params, timeout))
            return {
                "current": {
                    "temperature_2m": 19.25,
                    "wind_speed_10m": 12.5,
                    "wind_direction_10m": 225.0,
                }
            }

        collector = WeatherCollector(request=request)
        result = collector.collect()

        self.assertEqual(result.temperature_c, 19.25)
        self.assertEqual(requested, [
            ("https://api.open-meteo.com/v1/forecast", {
                "latitude": str(BERLIN_LATITUDE),
                "longitude": str(BERLIN_LONGITUDE),
                "current": "temperature_2m,wind_speed_10m,wind_direction_10m",
            }, 20.0),
        ])

    def test_maps_current_weather_to_valid_numeric_protobuf_records(self) -> None:
        records = weather_measurements(
            temperature_c=19.25,
            wind_speed_kmh=12.5,
            wind_direction_degrees=225.0,
            observed_at=datetime(2026, 9, 25, 10, 30, tzinfo=timezone.utc),
        )

        self.assertEqual(set(records), set(WEATHER_MRIDS))
        for name, record in records.items():
            encoded = record.SerializeToString()
            self.assertTrue(encoded)
            self.assertEqual(record.mrid, WEATHER_MRIDS[name])
            self.assertEqual(record.WhichOneof("value"), "double_value")
            self.assertTrue(record.quality.valid)
            self.assertEqual(record.timestamp_mccs.seconds, 1_790_332_200)

    def test_rejects_missing_or_non_numeric_open_meteo_current_values(self) -> None:
        collector = WeatherCollector(request=lambda *_: {"current": {"temperature_2m": 19.25}})

        with self.assertRaisesRegex(WeatherCollectionError, "wind_speed_10m"):
            collector.collect()


if __name__ == "__main__":
    unittest.main()
