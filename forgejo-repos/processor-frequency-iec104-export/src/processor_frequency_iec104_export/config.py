"""Environment-backed direct-frequency IEC 104 export settings."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import os


class ConfigurationError(ValueError):
    """Raised when direct frequency export settings are incomplete or unsafe."""


_MONITOR_CAUSE_CODES = frozenset({1, 2, 3, 4, 5, *range(20, 37)})


@dataclass(frozen=True)
class Settings:
    """Kafka and explicit IEC mapping inputs for the direct PoC slice."""

    common_address: int
    consumer_group: str
    cause_code: int
    information_object_address: int
    input_topic: str
    kafka_bootstrap_servers: str
    output_topic: str
    source_mrid: str

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> "Settings":
        """Load the explicit direct-PoC frequency-to-IEC mapping."""

        values = os.environ if environment is None else environment
        cause_code = _integer(values, "FREQUENCY_IEC104_CAUSE_CODE", 3, 1, 63)
        if cause_code not in _MONITOR_CAUSE_CODES:
            raise ConfigurationError("FREQUENCY_IEC104_CAUSE_CODE must be monitor-direction compatible")
        return cls(
            common_address=_integer(values, "FREQUENCY_IEC104_COMMON_ADDRESS", 1, 1, 65_535),
            consumer_group=_required(
                values,
                "WAMA_PROCESSOR_CONSUMER_GROUP",
                "processor-frequency-iec104-export",
            ),
            cause_code=cause_code,
            information_object_address=_integer(
                values,
                "FREQUENCY_IEC104_INFORMATION_OBJECT_ADDRESS",
                1001,
                1,
                16_777_215,
            ),
            input_topic=_required(values, "WAMA_INPUT_TOPIC", "LiveMeasurement"),
            kafka_bootstrap_servers=_required(
                values,
                "WAMA_KAFKA_BOOTSTRAP_SERVERS",
                "kafka:9092",
            ),
            output_topic=_required(values, "WAMA_OUTPUT_TOPIC", "Export"),
            source_mrid=_required(
                values,
                "FREQUENCY_IEC104_SOURCE_MRID",
                "urn:wama:poc:pmu:bay-01:frequency",
            ),
        )


def _required(values: Mapping[str, str], name: str, default: str) -> str:
    value = values.get(name, default).strip()
    if not value:
        raise ConfigurationError(f"{name} must not be empty")
    return value


def _integer(
    values: Mapping[str, str],
    name: str,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    try:
        value = int(values.get(name, str(default)))
    except ValueError as error:
        raise ConfigurationError(f"{name} must be an integer") from error
    if not minimum <= value <= maximum:
        raise ConfigurationError(f"{name} must be between {minimum} and {maximum}")
    return value