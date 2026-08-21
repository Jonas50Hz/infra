"""Verify one completed MeasurementSession through Trino federation."""

from __future__ import annotations

import os
from typing import Any
from uuid import UUID

from infra_readiness.config import ConfigurationError, Settings
from infra_readiness.trino import TrinoClient, TrinoReadinessError, check_trino


class FederationVerificationError(RuntimeError):
    """Raised when Trino does not preserve immutable session coverage evidence."""


def main() -> int:
    """Check a completed session's PostgreSQL coverage against Druid through Trino."""

    try:
        settings = Settings.from_environment()
        session_id = _session_id(os.environ.get("TRINO_COMPLETE_SESSION_ID", ""))
        check_trino(settings)
        client = TrinoClient(settings.trino_url, settings.trino_user)
        try:
            result = client.execute(_coverage_query(settings, session_id))
        finally:
            client.close()
        _validate_coverage(result.rows, result.error)
    except (ConfigurationError, FederationVerificationError, TrinoReadinessError) as error:
        raise SystemExit(f"Trino federation validation failed: {error}") from error

    print(f"Trino federation validation passed for completed session {session_id}.")
    return 0


def _session_id(value: str) -> str:
    try:
        parsed = UUID(value)
    except ValueError as error:
        raise FederationVerificationError("TRINO_COMPLETE_SESSION_ID must be a canonical UUID") from error
    if str(parsed) != value:
        raise FederationVerificationError("TRINO_COMPLETE_SESSION_ID must be a canonical UUID")
    return value


def _coverage_query(settings: Settings, session_id: str) -> str:
    return (
        "SELECT coverage.mrid, coverage.measurement_count AS expected_count, "
        "count(live.mrid) AS actual_count "
        f"FROM {settings.trino_blobmeta_catalog}.{settings.trino_blobmeta_schema}.session_blobs AS blobs "
        f"JOIN {settings.trino_blobmeta_catalog}.{settings.trino_blobmeta_schema}.session_blob_mrids AS coverage "
        "ON coverage.blob_id = blobs.blob_id "
        f"LEFT JOIN {settings.trino_druid_catalog}.{settings.trino_druid_schema}.live_measurements AS live "
        "ON live.mrid = coverage.mrid "
        'AND live."__time" >= CAST(blobs.started_at AS timestamp) '
        'AND live."__time" < CAST(blobs.ended_at AS timestamp) '
        f"WHERE blobs.session_id = UUID '{session_id}' "
        "AND blobs.status = 'COMPLETE' "
        "GROUP BY coverage.mrid, coverage.measurement_count "
        "ORDER BY coverage.mrid"
    )


def _validate_coverage(rows: tuple[tuple[Any, ...], ...], error: str | None) -> None:
    if error is not None:
        raise FederationVerificationError(f"cross-catalog coverage query failed: {error}")
    if len(rows) != 1:
        raise FederationVerificationError("cross-catalog coverage query did not return one MRID row")
    mrid, expected_count, actual_count = rows[0]
    if not isinstance(mrid, str) or not mrid:
        raise FederationVerificationError("cross-catalog coverage query returned an invalid MRID")
    if isinstance(expected_count, bool) or not isinstance(expected_count, int) or expected_count < 1:
        raise FederationVerificationError("Blobmeta coverage count is invalid")
    if isinstance(actual_count, bool) or not isinstance(actual_count, int):
        raise FederationVerificationError("Druid coverage count is invalid")
    if actual_count != expected_count:
        raise FederationVerificationError(
            f"Druid row count {actual_count} does not match Blobmeta coverage {expected_count}"
        )


if __name__ == "__main__":
    raise SystemExit(main())