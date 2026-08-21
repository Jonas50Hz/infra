"""Compare approved Git masterdata with the compacted Kafka projection."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime

from gateway_c37_118_onboarding.codec import (
    MasterdataCodecError,
    build_source_message,
    decode_source_message,
    serialize_source_message,
)
from gateway_c37_118_onboarding.config import Catalog, SourceDefinition
from gateway_c37_118_onboarding.generated import masterdata_pb2


class ReconciliationError(ValueError):
    """Raised when a proposed catalog would violate established identities."""


@dataclass(frozen=True)
class PublishRecord:
    """One non-null compacted source record that must be acknowledged by Kafka."""

    source_id: str
    payload: bytes


@dataclass(frozen=True)
class ReconciliationPlan:
    """The complete, deterministic update required for one catalog revision."""

    upserts: tuple[PublishRecord, ...]
    tombstones: tuple[str, ...]


def reconcile_catalog(
    catalog: Catalog,
    existing_records: Mapping[str, bytes | None],
    published_at: datetime,
) -> ReconciliationPlan:
    """Build safe source upserts and catalog-owned source tombstones."""

    existing = _decode_existing_records(existing_records)
    proposed_sources = _index_proposed_sources(catalog)
    existing_mrid_owners = _index_existing_mrids(existing)

    for source_id, source in proposed_sources.items():
        previous = existing.get(source_id)
        if previous is not None:
            if previous.catalog_id != catalog.catalog_id:
                raise ReconciliationError(
                    f"source_id {source_id!r} is owned by catalog {previous.catalog_id!r}"
                )
            _validate_immutable_mrids(source, previous)
        _validate_mrid_ownership(source, existing_mrid_owners)

    upserts = tuple(
        PublishRecord(
            source_id=source.source_id,
            payload=serialize_source_message(
                build_source_message(
                    source,
                    catalog.catalog_id,
                    catalog.catalog_revision,
                    published_at,
                )
            ),
        )
        for source in sorted(proposed_sources.values(), key=lambda item: item.source_id)
    )
    tombstones = tuple(
        sorted(
            source_id
            for source_id, source in existing.items()
            if source.catalog_id == catalog.catalog_id and source_id not in proposed_sources
        )
    )
    return ReconciliationPlan(upserts=upserts, tombstones=tombstones)


def _decode_existing_records(
    records: Mapping[str, bytes | None],
) -> dict[str, masterdata_pb2.SourceMasterdata]:
    decoded: dict[str, masterdata_pb2.SourceMasterdata] = {}
    for source_id, payload in records.items():
        if not source_id:
            raise ReconciliationError("Compacted Masterdata state has an empty source key")
        if payload is None:
            continue
        try:
            decoded[source_id] = decode_source_message(source_id.encode("utf-8"), payload)
        except MasterdataCodecError as error:
            raise ReconciliationError(
                f"Existing Masterdata record for {source_id!r} is invalid: {error}"
            ) from error
    return decoded


def _index_proposed_sources(catalog: Catalog) -> dict[str, SourceDefinition]:
    sources: dict[str, SourceDefinition] = {}
    mrids: set[str] = set()
    for source in catalog.sources:
        if source.source_id in sources:
            raise ReconciliationError(f"Catalog repeats source_id {source.source_id!r}")
        sources[source.source_id] = source
        for signal in source.signals:
            if signal.mrid in mrids:
                raise ReconciliationError(f"Catalog repeats MRID {signal.mrid!r}")
            mrids.add(signal.mrid)
    return sources


def _index_existing_mrids(
    sources: Mapping[str, masterdata_pb2.SourceMasterdata],
) -> dict[str, tuple[str, str]]:
    owners: dict[str, tuple[str, str]] = {}
    for source_id, source in sources.items():
        for signal in source.signals:
            owner = owners.setdefault(signal.mrid, (source_id, signal.signal_id))
            if owner != (source_id, signal.signal_id):
                raise ReconciliationError(
                    f"Existing Masterdata state assigns MRID {signal.mrid!r} more than once"
                )
    return owners


def _validate_immutable_mrids(
    proposed: SourceDefinition,
    previous: masterdata_pb2.SourceMasterdata,
) -> None:
    previous_by_signal = {signal.signal_id: signal.mrid for signal in previous.signals}
    for signal in proposed.signals:
        previous_mrid = previous_by_signal.get(signal.signal_id)
        if previous_mrid is not None and previous_mrid != signal.mrid:
            raise ReconciliationError(
                f"MRID for {proposed.source_id!r}/{signal.signal_id!r} is immutable "
                f"({previous_mrid!r} cannot become {signal.mrid!r})"
            )


def _validate_mrid_ownership(
    proposed: SourceDefinition,
    owners: Mapping[str, tuple[str, str]],
) -> None:
    for signal in proposed.signals:
        owner = owners.get(signal.mrid)
        if owner is None:
            continue
        if owner[0] != proposed.source_id:
            raise ReconciliationError(
                f"MRID {signal.mrid!r} is already owned by {owner[0]!r}/{owner[1]!r}"
            )
        if owner[1] != signal.signal_id:
            raise ReconciliationError(
                f"MRID {signal.mrid!r} cannot move from {owner[1]!r} to {signal.signal_id!r}"
            )