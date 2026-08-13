"""Container entry point for the configurable fake PMU gateway."""

from __future__ import annotations

import logging
import os
from time import monotonic, sleep

from kafka import KafkaProducer

from pmu_gateway.config import ConfigurationError, load_config
from pmu_gateway.publisher import MeasurementPublisher

LOGGER = logging.getLogger(__name__)


def main() -> None:
    """Load the fixture once, then continuously publish its measurement batch."""

    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logging.getLogger("kafka").setLevel(logging.WARNING)

    config_path = os.environ.get("PMU_GATEWAY_CONFIG_PATH", "/etc/wama/messages.yaml")
    try:
        config = load_config(
            config_path,
            os.environ.get("PMU_GATEWAY_PUBLISH_INTERVAL_MS"),
        )
    except ConfigurationError as error:
        LOGGER.error("Invalid PMU gateway startup configuration: %s", error)
        raise SystemExit(2) from error

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.environ.get("KAFKA_TOPIC", "LiveMeasurement")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        client_id="pmu-gateway",
        acks="all",
        retries=10,
    )
    publisher = MeasurementPublisher(producer, topic)
    interval_seconds = config.publish_interval_ms / 1_000

    LOGGER.info(
        "Starting PMU gateway with %s configured message(s), interval %sms, topic %s",
        len(config.messages),
        config.publish_interval_ms,
        topic,
    )

    try:
        while True:
            cycle_started = monotonic()
            count = publisher.publish_cycle(config.messages)
            LOGGER.info("Published %s configured measurement(s) to %s", count, topic)
            remaining_interval = interval_seconds - (monotonic() - cycle_started)
            if remaining_interval > 0:
                sleep(remaining_interval)
    finally:
        producer.close()


if __name__ == "__main__":
    main()