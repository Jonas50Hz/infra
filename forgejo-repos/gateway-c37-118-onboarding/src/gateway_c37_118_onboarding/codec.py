"""Raw-Protobuf encoding and defensive decoding for Masterdata records."""

from __future__ import annotations

from datetime import datetime, timezone

from google.protobuf.message import DecodeError

from gateway_c37_118_onboarding.config import SourceDefinition
from gateway_c37_118_onboarding.generated import masterdata_pb2


VALUE_KIND_ENUMS = {
    "double": masterdata_pb2.MCCS_VALUE_KIND_DOUBLE,
    "int": masterdata_pb2.MCCS_VALUE_KIND_INT,
    "uint": masterdata_pb2.MCCS_VALUE_KIND_UINT,
    "bool": masterdata_pb2.MCCS_VALUE_KIND_BOOL,
    "string": masterdata_pb2.MCCS_VALUE_KIND_STRING,
    "timestamp": masterdata_pb2.MCCS_VALUE_KIND_TIMESTAMP,
}


class MasterdataCodecError(ValueError):
    """Raised when a compacted Masterdata record is malformed or unsafe."""


def build_source_message(
    source: SourceDefinition,
    catalog_id: str,
    catalog_revision: str,
    published_at: datetime,
) -> masterdata_pb2.SourceMasterdata:
    """Translate one validated source into its canonical runtime projection."""

    if published_at.tzinfo is None:
        raise MasterdataCodecError("published_at must include a timezone")
    message = masterdata_pb2.SourceMasterdata(
        source_id=source.source_id,
        catalog_id=catalog_id,
        catalog_revision=catalog_revision,
    )
    message.published_at.FromDatetime(published_at.astimezone(timezone.utc))
    message.location.site_id = source.site_id
    message.location.display_name = source.display_name
    message.c37_118_tcp.ip_address = source.ip_address
    message.c37_118_tcp.port = source.port
    message.c37_118_tcp.pmu_idcode = source.pmu_idcode
    message.c37_118_tcp.wire_version = source.wire_version
    for definition in source.signals:
        signal = message.signals.add()
        signal.signal_id = definition.signal_id
        signal.source_channel = definition.source_channel
        signal.mrid = definition.mrid
        signal.value_kind = VALUE_KIND_ENUMS[definition.value_kind]
        signal.quantity = definition.quantity
        signal.unit = definition.unit
        selector = definition.c37_118_v2_selector
        if selector.kind == "phasor_magnitude":
            signal.c37_118_v2_selector.phasor_magnitude_channel = (
                selector.phasor_magnitude_channel or ""
            )
        elif selector.kind == "frequency":
            signal.c37_118_v2_selector.frequency = True
        elif selector.kind == "rocof":
            signal.c37_118_v2_selector.rocof = True
        else:
            raise MasterdataCodecError("Source signal has an unsupported C37.118 v2 selector")
    return message


def serialize_source_message(message: masterdata_pb2.SourceMasterdata) -> bytes:
    """Serialize a record deterministically for stable Kafka reconciliation."""

    return message.SerializeToString(deterministic=True)


def decode_source_message(key: bytes, payload: bytes) -> masterdata_pb2.SourceMasterdata:
    """Decode an existing non-null record and enforce its key identity."""

    source_id = _decode_source_key(key)
    message = masterdata_pb2.SourceMasterdata()
    try:
        message.ParseFromString(payload)
    except DecodeError as error:
        raise MasterdataCodecError("Masterdata value is not raw Protobuf") from error
    if message.source_id != source_id or not message.source_id:
        raise MasterdataCodecError("Masterdata Kafka key must equal message.source_id")
    if not message.catalog_id or not message.catalog_revision:
        raise MasterdataCodecError("Masterdata record has no catalog provenance")
    if message.WhichOneof("connection") != "c37_118_tcp":
        raise MasterdataCodecError("Masterdata record has no C37.118 TCP connection")
    wire_version = message.c37_118_tcp.wire_version
    if wire_version not in (
        masterdata_pb2.C37_118_WIRE_VERSION_UNSPECIFIED,
        masterdata_pb2.C37_118_WIRE_VERSION_2,
    ):
        raise MasterdataCodecError("Masterdata record has an unsupported C37.118 wire version")
    signal_ids: set[str] = set()
    mrids: set[str] = set()
    for signal in message.signals:
        if not signal.signal_id or not signal.mrid:
            raise MasterdataCodecError("Masterdata record has an incomplete signal mapping")
        if signal.signal_id in signal_ids or signal.mrid in mrids:
            raise MasterdataCodecError("Masterdata record repeats a signal ID or MRID")
        selector = signal.c37_118_v2_selector.WhichOneof("selector")
        if wire_version == masterdata_pb2.C37_118_WIRE_VERSION_2 and selector is None:
            raise MasterdataCodecError("Masterdata v2 signal has no C37.118 selector")
        if selector == "phasor_magnitude_channel" and not signal.c37_118_v2_selector.phasor_magnitude_channel:
            raise MasterdataCodecError("Masterdata v2 phasor selector has no channel")
        if selector in {"frequency", "rocof"} and not getattr(signal.c37_118_v2_selector, selector):
            raise MasterdataCodecError("Masterdata v2 singleton selector must be true")
        signal_ids.add(signal.signal_id)
        mrids.add(signal.mrid)
    return message


def _decode_source_key(key: bytes) -> str:
    if not key:
        raise MasterdataCodecError("Masterdata record has no Kafka key")
    try:
        source_id = key.decode("utf-8")
    except UnicodeDecodeError as error:
        raise MasterdataCodecError("Masterdata Kafka key must be UTF-8") from error
    if not source_id:
        raise MasterdataCodecError("Masterdata record has an empty Kafka key")
    return source_id