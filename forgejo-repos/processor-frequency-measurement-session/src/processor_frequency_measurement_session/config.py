"""Runtime settings for the isolated frequency-session processor."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os


class ConfigurationError(ValueError):
    """Raised when processor runtime settings are incomplete."""


@dataclass(frozen=True)
class Settings:
    """Kafka settings for the fixed reviewed Python capture policy."""

    consumer_group: str
    input_topic: str
    kafka_bootstrap_servers: str
    output_topic: str

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> "Settings":
        """Load Kafka connectivity without accepting dynamic capture configuration."""

        values = os.environ if environment is None else environment
        return cls(
            consumer_group=_required(
                values,
                "WAMA_PROCESSOR_CONSUMER_GROUP",
                "processor-frequency-measurement-session",
            ),
            input_topic=_required(values, "WAMA_INPUT_TOPIC", "LiveMeasurement"),
            kafka_bootstrap_servers=_required(
                values,
                "WAMA_KAFKA_BOOTSTRAP_SERVERS",
                "kafka:9092",
            ),
            output_topic=_required(values, "WAMA_OUTPUT_TOPIC", "MeasurementSession"),
        )


def _required(values: Mapping[str, str], name: str, default: str) -> str:
    value = values.get(name, default).strip()
    if not value:
        raise ConfigurationError(f"{name} must not be empty")
    return value