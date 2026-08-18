"""Configuration parsing for one immutable finalized-session export."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timezone
import os
from pathlib import Path
from typing import Any

import yaml


class ConfigurationError(ValueError):
    """Raised when exporter configuration cannot produce a final session."""


@dataclass(frozen=True)
class ExporterSettings:
    """Environment-owned runtime endpoints and credentials."""

    kafka_bootstrap_servers: str
    kafka_topic: str
    fixture_path: Path
    s3_access_key_id: str
    s3_bucket: str
    s3_endpoint_url: str
    s3_region: str
    s3_secret_access_key: str
    session_id_override: str | None

    @classmethod
    def from_environment(cls, environment: dict[str, str] | None = None) -> "ExporterSettings":
        """Load trusted-PoC settings without embedding them in application code."""

        values = os.environ if environment is None else environment
        override = values.get("MEASUREMENT_SESSION_ID_OVERRIDE", "").strip() or None
        return cls(
            kafka_bootstrap_servers=_required(values, "KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            kafka_topic=_required(values, "KAFKA_TOPIC", "MeasurementSession"),
            fixture_path=Path(
                _required(
                    values,
                    "MEASUREMENT_SESSION_FIXTURE_PATH",
                    "/app/fixture/finalized-session.yaml",
                )
            ),
            s3_access_key_id=_required(values, "S3_ACCESS_KEY_ID", "wama-s3-admin"),
            s3_bucket=_required(values, "S3_BUCKET", "wama-measurement-sessions"),
            s3_endpoint_url=_url(values, "S3_ENDPOINT_URL", "http://seaweedfs:8333"),
            s3_region=_required(values, "S3_REGION", "us-east-1"),
            s3_secret_access_key=_required(
                values,
                "S3_SECRET_ACCESS_KEY",
                "wama-s3-admin-secret",
            ),
            session_id_override=override,
        )


@dataclass(frozen=True)
class ArtifactFixture:
    """An artifact file that becomes an immutable manifest descriptor."""

    artifact_id: str
    content_type: str
    path: Path


@dataclass(frozen=True)
class FinalizedSessionFixture:
    """Fully bounded final-session data loaded before external side effects."""

    session_id: str
    source_mrid: str
    started_at: datetime
    ended_at: datetime
    finalized_at: datetime
    measurement_count: int
    metadata: tuple[tuple[str, str], ...]
    artifacts: tuple[ArtifactFixture, ...]


def load_fixture(path: Path, session_id_override: str | None = None) -> FinalizedSessionFixture:
    """Load one static finalized-session fixture and resolve only local artifacts."""

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise ConfigurationError(f"Unable to read finalized-session fixture {path}: {error}") from error
    except yaml.YAMLError as error:
        raise ConfigurationError(f"Unable to parse finalized-session fixture {path}: {error}") from error

    root = _mapping(raw, "fixture")
    _reject_unknown(root, {"session", "artifacts"}, "fixture")
    session = _mapping(root.get("session"), "fixture.session")
    _reject_unknown(
        session,
        {
            "session_id",
            "source_mrid",
            "started_at",
            "ended_at",
            "finalized_at",
            "measurement_count",
            "metadata",
        },
        "fixture.session",
    )
    fixture_directory = path.resolve().parent
    fixture = FinalizedSessionFixture(
        session_id=_required_string(session, "session_id", "fixture.session"),
        source_mrid=_required_string(session, "source_mrid", "fixture.session"),
        started_at=_timestamp(session, "started_at"),
        ended_at=_timestamp(session, "ended_at"),
        finalized_at=_timestamp(session, "finalized_at"),
        measurement_count=_nonnegative_integer(session, "measurement_count", "fixture.session"),
        metadata=_metadata(session.get("metadata")),
        artifacts=_artifacts(root.get("artifacts"), fixture_directory),
    )
    return replace(fixture, session_id=session_id_override) if session_id_override else fixture


def _artifacts(value: Any, fixture_directory: Path) -> tuple[ArtifactFixture, ...]:
    if not isinstance(value, list) or not value:
        raise ConfigurationError("fixture.artifacts must be a non-empty list")
    artifacts: list[ArtifactFixture] = []
    for index, raw_artifact in enumerate(value):
        artifact = _mapping(raw_artifact, f"fixture.artifacts[{index}]")
        _reject_unknown(artifact, {"id", "path", "content_type"}, f"fixture.artifacts[{index}]")
        relative_path = _required_string(artifact, "path", f"fixture.artifacts[{index}]")
        resolved_path = (fixture_directory / relative_path).resolve()
        if not resolved_path.is_relative_to(fixture_directory) or not resolved_path.is_file():
            raise ConfigurationError(f"fixture artifact path is not a local file: {relative_path}")
        artifacts.append(
            ArtifactFixture(
                artifact_id=_required_string(artifact, "id", f"fixture.artifacts[{index}]"),
                content_type=_required_string(
                    artifact,
                    "content_type",
                    f"fixture.artifacts[{index}]",
                ),
                path=resolved_path,
            )
        )
    return tuple(artifacts)


def _metadata(value: Any) -> tuple[tuple[str, str], ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise ConfigurationError("fixture.session.metadata must be a list")
    entries: list[tuple[str, str]] = []
    for index, raw_entry in enumerate(value):
        entry = _mapping(raw_entry, f"fixture.session.metadata[{index}]")
        _reject_unknown(entry, {"key", "value"}, f"fixture.session.metadata[{index}]")
        entries.append(
            (
                _required_string(entry, "key", f"fixture.session.metadata[{index}]"),
                _required_string(entry, "value", f"fixture.session.metadata[{index}]"),
            )
        )
    return tuple(entries)


def _timestamp(values: dict[str, Any], name: str) -> datetime:
    raw = _required_string(values, name, "fixture.session")
    try:
        timestamp = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as error:
        raise ConfigurationError(f"fixture.session.{name} must be RFC 3339") from error
    if timestamp.tzinfo is None:
        raise ConfigurationError(f"fixture.session.{name} must include a timezone")
    return timestamp.astimezone(timezone.utc)


def _mapping(value: Any, location: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigurationError(f"{location} must be a mapping")
    return value


def _reject_unknown(values: dict[str, Any], allowed: set[str], location: str) -> None:
    unknown = set(values).difference(allowed)
    if unknown:
        raise ConfigurationError(f"{location} contains unsupported key(s): {', '.join(sorted(unknown))}")


def _required_string(values: dict[str, Any], name: str, location: str) -> str:
    value = values.get(name)
    if not isinstance(value, str) or not value.strip():
        raise ConfigurationError(f"{location}.{name} must be a non-empty string")
    return value.strip()


def _nonnegative_integer(values: dict[str, Any], name: str, location: str) -> int:
    value = values.get(name)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ConfigurationError(f"{location}.{name} must be a non-negative integer")
    return value


def _required(values: dict[str, str], name: str, default: str) -> str:
    value = values.get(name, default).strip()
    if not value:
        raise ConfigurationError(f"{name} must not be empty")
    return value


def _url(values: dict[str, str], name: str, default: str) -> str:
    value = _required(values, name, default).rstrip("/")
    if not value.startswith(("http://", "https://")):
        raise ConfigurationError(f"{name} must be an HTTP URL")
    return value