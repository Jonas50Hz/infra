"""Dynamic scoped Masterdata membership for reviewed alarm rules."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Iterable

from google.protobuf.message import DecodeError

from processor_alarm_threshold.config import AlarmRule
from processor_alarm_threshold.generated import masterdata_pb2


class MasterdataError(ValueError):
    """Raised when scoped Masterdata cannot safely define alarm membership."""


_SOURCE_ID = re.compile(r"[a-z0-9][a-z0-9-]{0,62}\Z")


@dataclass(frozen=True)
class MembershipChange:
    """The exact eligible MRIDs added and removed by one source update."""

    added: frozenset[str]
    removed: frozenset[str]


@dataclass(frozen=True)
class _ScopedSource:
    source_id: str
    mrids: frozenset[str]


class MasterdataRegistry:
    """Fold compacted source metadata into unique exact eligible MRIDs."""

    def __init__(self, catalog_id: str, rules: Iterable[AlarmRule]) -> None:
        self._catalog_id = catalog_id
        self._rules = tuple(rules)
        self._sources: dict[str, _ScopedSource] = {}

    @property
    def mrids(self) -> frozenset[str]:
        """Return all currently eligible exact MRIDs."""

        return frozenset(
            mrid
            for source in self._sources.values()
            for mrid in source.mrids
        )

    def rules_for(self, mrid: str) -> tuple[AlarmRule, ...]:
        """Return reviewed rules that currently own one exact MRID."""

        if mrid not in self.mrids:
            return ()
        return tuple(rule for rule in self._rules if rule.selector.matches_mrid(mrid))

    def apply(self, key: bytes | None, value: bytes | None) -> MembershipChange:
        """Apply a compacted source upsert or tombstone atomically."""

        source_id = _decode_source_id(key)
        source = _decode_scoped_source(source_id, value, self._catalog_id, self._rules)
        candidate = dict(self._sources)
        if source is None:
            candidate.pop(source_id, None)
        else:
            candidate[source_id] = source
        _validate_unique_mrids(candidate.values())
        previous_mrids = self.mrids
        self._sources = candidate
        current_mrids = self.mrids
        return MembershipChange(
            added=current_mrids.difference(previous_mrids),
            removed=previous_mrids.difference(current_mrids),
        )


def _decode_source_id(key: bytes | None) -> str:
    if not isinstance(key, bytes) or not key:
        raise MasterdataError("Masterdata record has no UTF-8 source key")
    try:
        source_id = key.decode("utf-8")
    except UnicodeDecodeError as error:
        raise MasterdataError("Masterdata source key is not UTF-8") from error
    if not _SOURCE_ID.fullmatch(source_id):
        raise MasterdataError("Masterdata source_id has an unsupported format")
    return source_id


def _decode_scoped_source(
    source_id: str,
    value: bytes | None,
    catalog_id: str,
    rules: tuple[AlarmRule, ...],
) -> _ScopedSource | None:
    if value is None:
        return None
    message = masterdata_pb2.SourceMasterdata()
    try:
        message.ParseFromString(value)
    except DecodeError as error:
        raise MasterdataError("Masterdata value is not valid raw Protobuf") from error
    if message.source_id != source_id:
        raise MasterdataError("Masterdata Kafka key must equal message.source_id")
    if message.catalog_id != catalog_id:
        return None
    if not message.catalog_revision or not message.HasField("published_at"):
        raise MasterdataError("scoped Masterdata lacks catalog provenance")
    try:
        message.published_at.ToDatetime()
    except ValueError as error:
        raise MasterdataError("scoped Masterdata publication timestamp is invalid") from error
    mrids: set[str] = set()
    for signal in message.signals:
        matching_rules = tuple(rule for rule in rules if rule.selector.matches_mrid(signal.mrid))
        if not matching_rules:
            continue
        if signal.mrid in mrids:
            raise MasterdataError("scoped Masterdata repeats an eligible exact MRID")
        if (
            signal.value_kind != masterdata_pb2.MCCS_VALUE_KIND_DOUBLE
            or signal.quantity != "frequency"
            or signal.unit != "Hz"
        ):
            raise MasterdataError(
                "scoped Masterdata has incompatible double/frequency/Hz signal semantics"
            )
        mrids.add(signal.mrid)
    return _ScopedSource(source_id=source_id, mrids=frozenset(mrids))


def _validate_unique_mrids(sources: Iterable[_ScopedSource]) -> None:
    owners: dict[str, str] = {}
    for source in sources:
        for mrid in source.mrids:
            existing = owners.get(mrid)
            if existing is not None and existing != source.source_id:
                raise MasterdataError(
                    f"scoped Masterdata exact MRID {mrid!r} belongs to multiple sources"
                )
            owners[mrid] = source.source_id