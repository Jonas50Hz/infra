"""Publish Open-Meteo weather as raw Common Format records every five minutes."""

from __future__ import annotations

import logging
import os
import time

from confluent_kafka import Producer

from processor_weather_map.weather import WeatherCollectionError, WeatherCollector, weather_measurements

POLL_INTERVAL_SECONDS = 300
LOGGER = logging.getLogger(__name__)


def main() -> None:
    """Run the managed collector; failures are logged and retried next interval."""

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    bootstrap_servers = os.environ.get("WAMA_KAFKA_BOOTSTRAP_SERVERS", "kafka:9092").strip()
    topic = os.environ.get("WAMA_OUTPUT_TOPIC", "LiveMeasurement").strip()
    if not bootstrap_servers or not topic:
        raise ValueError("WAMA_KAFKA_BOOTSTRAP_SERVERS and WAMA_OUTPUT_TOPIC must be non-empty")
    producer = Producer({"bootstrap.servers": bootstrap_servers, "client.id": "processor-weather-map"})
    collector = WeatherCollector()
    while True:
        try:
            reading = collector.collect()
            records = weather_measurements(
                temperature_c=reading.temperature_c,
                wind_speed_kmh=reading.wind_speed_kmh,
                wind_direction_degrees=reading.wind_direction_degrees,
            )
            for record in records.values():
                timestamp_ms = record.timestamp_mccs.seconds * 1_000 + record.timestamp_mccs.nanos // 1_000_000
                producer.produce(
                    topic,
                    key=record.mrid.encode("utf-8"),
                    value=record.SerializeToString(),
                    timestamp=timestamp_ms,
                )
            outstanding = producer.flush(20)
            if outstanding:
                raise RuntimeError(f"Kafka did not deliver {outstanding} weather records")
            LOGGER.info("Published Berlin weather records to %s", topic)
        except (WeatherCollectionError, RuntimeError) as error:
            LOGGER.warning("Weather collection/publish failed; retrying in five minutes: %s", error)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
