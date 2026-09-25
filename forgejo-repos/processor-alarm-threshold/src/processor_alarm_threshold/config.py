"""Strict reviewed configuration for stateful alarm threshold evaluation."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import math
import os
from pathlib import Path
from types import MappingProxyType
from typing import Any

import yaml


class ConfigurationError(ValueError):
    """Raised when reviewed alarm configuration is incomplete or unsafe."""


_SUPPORTED_SELECTOR = {
    "value_kind": "double",
    "quantity": "frequency",
    "unit": "Hz",
}
_RULE_IDENTIFIER_CHARACTERS = frozenset("abcdefghijklmnopqrstuvwxyz0123456789-")
_ALARM_EVALUATION_WATERMARK_MIGRATION_GUARD = (
    "accept-forward-only-alarm-evaluation-watermark-v1"
)


@dataclass(frozen=True)
class SignalSelector:
    """An anchored, segment-only MRID selector with exact signal semantics."""

    mrid_glob: str
    value_kind: str
    quantity: str
    unit: str

    def matches_mrid(self, mrid: str) -> bool:
        """Return whether an exact MRID satisfies this whole-segment glob."""

        pattern_segments = self.mrid_glob.split(":")
        mrid_segments = mrid.split(":")
        if len(pattern_segments) != len(mrid_segments) or any(not segment for segment in mrid_segments):
            return False
        return all(
            pattern == "*" or pattern == value
            for pattern, value in zip(pattern_segments, mrid_segments, strict=True)
        )


@dataclass(frozen=True)
class AlarmRule:
    """One reviewed threshold rule applied to dynamically catalogued signals."""

    rule_id: str
    revision: str
    selector: SignalSelector
    operator: str
    threshold: float
    severity: str


@dataclass(frozen=True)
class Settings:
    """Kafka endpoints and fully validated reviewed alarm rules."""

    alarm_topic: str
    catalog_id: str
    config_path: str
    consumer_group: str
    input_topic: str
    kafka_bootstrap_servers: str
    masterdata_topic: str
    rules: tuple[AlarmRule, ...]
    alarm_evaluation_watermark_topic: str = "AlarmEvaluationWatermark"
    allow_legacy_active_alarm_bootstrap: bool = False

    @classmethod
    def from_environment(cls, environment: Mapping[str, str] | None = None) -> "Settings":
        """Load runtime settings and a reviewed configuration from an absolute path."""

        values = os.environ if environment is None else environment
        config_path = _absolute_path(
            values,
            "ALARM_THRESHOLD_CONFIG_PATH",
            "/etc/wama/alarm-threshold.yaml",
        )
        reviewed = load_reviewed_rules(config_path)
        return cls(
            alarm_topic=_required(values, "WAMA_ALARM_TOPIC", "Alarm"),
            alarm_evaluation_watermark_topic=_required(
                values,
                "WAMA_ALARM_EVALUATION_WATERMARK_TOPIC",
                "AlarmEvaluationWatermark",
            ),
            allow_legacy_active_alarm_bootstrap=_migration_fence(values),
            catalog_id=reviewed.catalog_id,
            config_path=config_path,
            consumer_group=_required(
                values,
                "WAMA_PROCESSOR_CONSUMER_GROUP",
                "processor-alarm-threshold",
            ),
            input_topic=_required(values, "WAMA_INPUT_TOPIC", "LiveMeasurement"),
            kafka_bootstrap_servers=_required(
                values,
                "WAMA_KAFKA_BOOTSTRAP_SERVERS",
                "kafka:9092",
            ),
            masterdata_topic=_required(values, "WAMA_MASTERDATA_TOPIC", "Masterdata"),
            rules=reviewed.rules,
        )


@dataclass(frozen=True)
class ReviewedRules:
    """Validated root configuration loaded from the reviewed YAML file."""

    catalog_id: str
    rules: tuple[AlarmRule, ...]


def load_reviewed_rules(path: str | Path) -> ReviewedRules:
    """Load a versioned reviewed alarm policy without permissive YAML coercion."""

    config_path = Path(path)
    try:
        with config_path.open(encoding="utf-8") as config_file:
            raw_config = yaml.safe_load(config_file)
    except OSError as error:
        raise ConfigurationError(
            f"Unable to read alarm threshold configuration {config_path}: {error}"
        ) from error
    except yaml.YAMLError as error:
        raise ConfigurationError(
            f"Unable to parse alarm threshold configuration {config_path}: {error}"
        ) from error

    config = _mapping(raw_config, "configuration")
    _reject_unknown_keys(config, {"version", "catalog_id", "rules"}, "configuration")
    version = config.get("version")
    if isinstance(version, bool) or version != 1:
        raise ConfigurationError("configuration.version must be 1")
    catalog_id = _required_string(config, "catalog_id", "configuration")
    raw_rules = config.get("rules")
    if not isinstance(raw_rules, list) or not raw_rules:
        raise ConfigurationError("configuration.rules must be a non-empty list")

    rules: list[AlarmRule] = []
    rule_ids: set[str] = set()
    for index, raw_rule in enumerate(raw_rules):
        location = f"configuration.rules[{index}]"
        rule = _decode_rule(_mapping(raw_rule, location), location)
        if rule.rule_id in rule_ids:
            raise ConfigurationError(f"{location}.rule_id duplicates an existing rule")
        rule_ids.add(rule.rule_id)
        rules.append(rule)
    return ReviewedRules(catalog_id=catalog_id, rules=tuple(rules))


def _decode_rule(raw_rule: Mapping[str, Any], location: str) -> AlarmRule:
    _reject_unknown_keys(
        raw_rule,
        {"rule_id", "revision", "selector", "operator", "threshold", "severity"},
        location,
    )
    rule_id = _required_string(raw_rule, "rule_id", location)
    if (
        len(rule_id) > 128
        or rule_id.startswith("-")
        or rule_id.endswith("-")
        or any(character not in _RULE_IDENTIFIER_CHARACTERS for character in rule_id)
    ):
        raise ConfigurationError(f"{location}.rule_id has an unsupported format")
    revision = _required_string(raw_rule, "revision", location)
    selector = _decode_selector(
        _mapping(raw_rule.get("selector"), f"{location}.selector"),
        f"{location}.selector",
    )
    operator = _required_string(raw_rule, "operator", location)
    if operator != "gt":
        raise ConfigurationError(f"{location}.operator must be gt")
    threshold = _finite_number(raw_rule, "threshold", location)
    severity = _required_string(raw_rule, "severity", location)
    if severity not in {"warning", "critical"}:
        raise ConfigurationError(f"{location}.severity must be warning or critical")
    return AlarmRule(
        rule_id=rule_id,
        revision=revision,
        selector=selector,
        operator=operator,
        threshold=threshold,
        severity=severity,
    )


def _decode_selector(raw_selector: Mapping[str, Any], location: str) -> SignalSelector:
    _reject_unknown_keys(raw_selector, {"mrid_glob", *_SUPPORTED_SELECTOR}, location)
    mrid_glob = _required_string(raw_selector, "mrid_glob", location)
    _validate_mrid_glob(mrid_glob, location)
    values = {
        name: _required_string(raw_selector, name, location)
        for name in _SUPPORTED_SELECTOR
    }
    for name, expected in _SUPPORTED_SELECTOR.items():
        if values[name] != expected:
            raise ConfigurationError(f"{location}.{name} must be {expected!r}")
    return SignalSelector(mrid_glob=mrid_glob, **values)


def _validate_mrid_glob(value: str, location: str) -> None:
    segments = value.split(":")
    if len(segments) < 2 or segments[0] != "urn":
        raise ConfigurationError(f"{location}.mrid_glob must be a URN segment glob")
    for segment in segments:
        if segment == "*":
            continue
        if not segment or any(
            character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-"
            for character in segment
        ):
            raise ConfigurationError(
                f"{location}.mrid_glob must use only literal segments or whole-segment *"
            )


def _required(values: Mapping[str, str], name: str, default: str) -> str:
    value = values.get(name, default).strip()
    if not value:
        raise ConfigurationError(f"{name} must not be empty")
    return value


def _absolute_path(values: Mapping[str, str], name: str, default: str) -> str:
    value = _required(values, name, default)
    if not Path(value).is_absolute():
        raise ConfigurationError(f"{name} must be an absolute path")
    return value


def _migration_fence(values: Mapping[str, str]) -> bool:
    value = values.get("WAMA_ALARM_EVALUATION_WATERMARK_MIGRATION", "").strip()
    if not value:
        return False
    if value != _ALARM_EVALUATION_WATERMARK_MIGRATION_GUARD:
        raise ConfigurationError(
            "WAMA_ALARM_EVALUATION_WATERMARK_MIGRATION must be "
            f"{_ALARM_EVALUATION_WATERMARK_MIGRATION_GUARD!r} when set"
        )
    return True


def _finite_number(values: Mapping[str, Any], name: str, location: str) -> float:
    value = values.get(name)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigurationError(f"{location}.{name} must be a finite number")
    number = float(value)
    if not math.isfinite(number):
        raise ConfigurationError(f"{location}.{name} must be a finite number")
    return number


def _mapping(value: Any, location: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ConfigurationError(f"{location} must be a mapping")
    return MappingProxyType(dict(value))


def _reject_unknown_keys(mapping: Mapping[str, Any], allowed_keys: set[str], location: str) -> None:
    unknown_keys = set(mapping).difference(allowed_keys)
    if unknown_keys:
        names = ", ".join(sorted(str(key) for key in unknown_keys))
        raise ConfigurationError(f"{location} has unsupported field(s): {names}")


def _required_string(mapping: Mapping[str, Any], name: str, location: str) -> str:
    value = mapping.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(f"{location}.{name} must be a non-empty string")
    return value.strip()