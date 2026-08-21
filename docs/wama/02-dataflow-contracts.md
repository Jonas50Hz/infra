# WAMA Data Flow (Common Format)

Sources: **WAMA Platform Concept** (Gerbrand Jonas) — "Process — Live data",
"Process — MeasurementSession & alarm", and "Architecture at a glance"; the
[architecture image](image.png); and the [live-data BPMN](Livedaten_Prozess_WAMA.bpmn).

> The image and BPMN are authoritative for process/component vocabulary. The
> PoC data plane remains separate from Nexus-specific Concentrator/TimeStep,
> Confluent Schema Registry, and PMUReading/STAT choices. It normalises every
> source to **Common Format** and routes it through the topics below.

## Path
1. **Source → Gateway.** One gateway container per source (PMU, PQM,
   Faultrecorder, ...). Gateway connects, pulls the stream, and normalises to
   Common Format (`MCCSMeasurementValue`).
2. **Gateway → Kafka.** Normalised measurements published to `LiveMeasurement`.
3. **Processing.** Quixstreams processors consume `LiveMeasurement`, compute
  derived values, and write them back to Kafka. Producers may submit bounded
  `MeasurementSession` requests, and processors may emit `Alarm` and `Export`
  records. The planned
  [LFR per-second frequency provision](04-lfr-frequency-provision.md) processor
  begins at this Kafka boundary; source-protocol and PDC ingestion are outside
  that use-case specification.
4. **Storage.**
   - Druid directly ingests raw-Protobuf `MCCSMeasurementValue` records from
     `LiveMeasurement` with its Kafka and Protobuf extensions. It uses the
     canonical build-time descriptor rather than a Schema Registry, uses Kafka
     record time as `__time`, and retains a no-rollup `live_measurements`
     datasource.
   - Raw / waveform data goes to SeaweedFS (off Kafka). Raw is deleted after
     X weeks; aggregated live data is archived.
   - Measurement sessions go to long-term storage (not deleted after six
     weeks).
5. **Visualisation.** The Druid Router makes Common Format measurement data
  queryable now. Grafana's `WAMA Measurements / WAMA PMU Live Measurements`
  dashboard queries Druid for valid PMU voltage, current, frequency, and ROCOF
  trends. This is separate from the Compose PoC's
  Grafana-over-VictoriaMetrics infrastructure dashboards. Kafka exporter sends
  only operational broker/topic metadata there; no measurement, waveform,
  measurement-session, alarm, or Kafka message records are sent to
  VictoriaMetrics. Trino provides read-only federation over Druid, the
  PostgreSQL Blobmeta projection, and registered immutable session artifacts in
  Iceberg. Grafana's selected-session dashboard queries that read-only Trino
  path; alerting and broader cross-session analytics remain later work.
6. **Export.** IEC 104 exporter (real-time) and File Export (xlsx/csv, on a
   configured measurement session or manual selection). MQTT exporter for
   OT/EAS.

## Kafka topics
| Topic | Type | Contents |
|-------|------|----------|
| `LiveMeasurement` | stream | `MCCSMeasurementValue` (Common Format) + derived values |
| `MeasurementSession` | stream | Raw-Protobuf bounded historical extraction requests |
| `Alarm` | stream | Alarm records raised from measurement sessions |
| `Export` | stream | Typed raw-Protobuf `ExportRecord` values for one-way IEC 104 export; file/MQTT payloads remain future work |
| `Masterdata` | compacted | Raw-Protobuf `SourceMasterdata` source endpoint, location, PMU identity, and signal-to-MRID mappings |
| `Schema` | compacted | Common-Format schema definitions |
| `Blobmeta` | compacted | Raw-Protobuf immutable Parquet pointers, status, and MRID coverage |

PostgreSQL receives the immutable `blobmeta_catalog` projection from the
compacted `Blobmeta` topic. Kafka remains the source of truth. A future Kafka
Connector may mirror `Masterdata` and `Schema` separately; it is not part of
this PoC.

## Masterdata source contract
**`SourceMasterdata`**, proto3 package `wama.masterdata.v1`. Canonical file:
[`schema/masterdata.proto`](schema/masterdata.proto). The Kafka key is the
exact UTF-8 `source_id`; non-null values are deterministic raw-Protobuf source
records and a null-valued record with the same key is a decommissioning
tombstone.

The private `gateway-c37-118-onboarding` Git catalog is the reviewed authority.
Its Systemexperte-approved `main` revision publishes a `catalog_id`, Git
revision, and publication timestamp with every source projection. V1 supports
one legacy C37.118 wire-version-2 TCP PMU endpoint per source: literal
IPv4/IPv6 address, explicit TCP port, and PMU IDCODE. A source includes stable
site ID/display name and one or more stable logical signals. Each signal maps a
source channel and explicit v2 selector to an immutable MRID, Common Format
scalar kind, quantity, and unit.

The V1 catalog allows only `voltage`/`V`, `current`/`A`, `frequency`/`Hz`, and
`rocof`/`Hz/s` as `double_value` mappings. Voltage/current selectors name a
fixed CFG-2 phasor magnitude channel; frequency and ROCOF use their singleton
v2 source values. It rejects duplicate source IDs, source channels, selectors,
or MRIDs; malformed endpoints; a non-v2 wire version; a selector whose channel
or quantity does not agree; and an attempt to mutate a published
`(source_id, signal_id)` MRID. Endpoint and location changes remain valid
configuration updates. No device credentials or secrets enter Git, Kafka, or
the record contract; a future protocol extension may carry only a secret
reference.

The current publisher reads the compacted topic through its end offsets before
writing. It rejects a source key owned by another catalog or an MRID owned by
another active source, writes active records in deterministic source order, and
tombstones only sources previously owned by its catalog. The approved
onboarding deployment then renders one isolated adapter per active source and
removes only a matching previously managed adapter after its tombstone.

Each adapter requests C37.118 v2 CFG-2, derives its field mapping from that
configuration, consumes bounded TCP frames, and publishes raw-Protobuf
`MCCSMeasurementValue` records to `LiveMeasurement` keyed by MRID. It carries
the source timestamp as `timestamp_field` and receipt time as both gateway and
MCCS timestamps. Its conservative generic quality mapping sets `valid=false`
for non-good or unsynchronized v2 `STAT`, and sets `substituted=true` only for
test/inserted data. It does not retain raw `STAT` or time-quality evidence and
therefore does not satisfy the later LFR audit contract by itself.

## Common Format — the contract
**`MCCSMeasurementValue`**, proto3, package `rtd_schema.v1`. Canonical file:
[`schema/rtd_schema.proto`](schema/rtd_schema.proto). **Same schema as MCCS.**
Serialization: **raw Protobuf** (no Confluent Schema Registry).

### Fields
- `mrid` (string) — identifies the associated Measurement.
  **PoC uses our own MRIDs first**, not MCCS-issued ones.
- `timestamp_field` (optional) — value generated in the field.
- `timestamp_gateway` (optional) — transmitted by the gateway.
- `timestamp_mccs` (required) — initial generation time inside MCCS
  (= Kafka message timestamp).
- Invariant: `timestamp_field <= timestamp_gateway <= timestamp_mccs`.
- Fields 5-9 reserved for future timestamp extensions.
- `value` (oneof primitive):
  - `double_value` — floating point (active/reactive power, voltage, temp, wind).
  - `int_value` (int64) — ordered discrete states (tap position).
  - `uint_value` (uint32) — runtime-configurable **enumerated** states
    (switch/breaker status, energized state) via ValueToAlias mapping.
  - `bool_value` — binary states.
  - `string_value` — status/error/interlocking texts.
  - `timestamp_value` — a timestamp as a measurement.
- `quality` (Quality): `valid`, `substituted`, `operator_blocked`, `overflow`,
  `old_data` — all optional, list is extensible.

### Interpretation rule
Value meaning is resolved by **Master Data config in the consuming
module/service**, not hard-coded. `uint_value` enum meanings are bound at
engineering time via ValueToAlias.

### Druid live-measurements ingestion
The root-owned `druid` service builds a Protobuf descriptor from the canonical
schema and exposes only its Router API on port 8888. `druid-init` idempotently
submits the `live_measurements` Kafka supervisor after Druid is ready. The
supervisor keeps `mrid`, Kafka key/topic metadata, all scalar `oneof` values,
quality flags, and source timestamps queryable. It uses `queryGranularity: none`
and `rollup: false`; no Druid Schema Registry, Kafka ZooKeeper service, or JSON
translation path is introduced.

## IEC 104 Export contract
**`ExportRecord`**, proto3, package `wama.iec104.v1`. Canonical file:
[`schema/iec104_export.proto`](schema/iec104_export.proto). Serialization is raw
Protobuf on the existing `Export` topic. The Kafka key is the canonical
`export_id` UUID, and the Kafka record timestamp must equal `created_at` rounded
down to milliseconds.

Each record contains one typed `Iec104Asdu`: type identifier, common address,
monitor-direction cause of transmission, and ordered information objects with
three-octet addresses, values, and IEC quality bits. The current PoC accepts
only `M_SP_NA_1`, `M_DP_NA_1`, and `M_ME_NC_1`; it rejects commands,
interrogations, non-finite short floats, unsupported COTs, and ASDUs larger than
the IEC 104 limit. The exporter derives VSQ object count and sequence addressing
from the ordered objects. c104 owns APDU framing, sequence numbers, and its
default COT flags: originator address 0, test false, and negative confirmation
false.

`iec104-exporter` is the controlled-station server on plain TCP port 2404. It
only sends monitor-direction ASDUs to one active control-center connection and
only processes TCP/IEC transport control traffic inbound. Its TCP ingress guard
forwards only well-formed U (`STARTDT`, `STOPDT`, `TESTFR`) and S
acknowledgement frames, and closes any connection that sends a malformed control
frame or application I-frame before c104 can process or answer it. It exposes no
command, interrogation, parameterization, or other inbound application-ASDU
handler. A Kafka offset is committed only after c104 accepts the exact outbound
batch, so Kafka remains at-least-once and a reconnect/restart can resend a
record.

The profile-gated `iec104-receiver` is a test-only control center. It uses
`STARTDT` without a general interrogation, publishes unique fixture records,
and verifies the received ASDU fields. It then deliberately sends a raw
general-interrogation I-frame and requires connection closure without an
application response. `iec104-browser` is the on-demand read-only control center
for operator viewing: an open page starts its `STARTDT` connection and receives
live values over IEC 104; the browser page owns the transient event list and
discarding the final page ends reception. It is the same single control-center
slot used by the profile-gated receiver, so browser and receiver tests do not
run concurrently. No export-producing processor exists yet; a future processor
owns its own deliberate copy of this canonical contract in its Forgejo
repository.

## MeasurementSession request and Blobmeta result contracts
**`MeasurementSessionRequest`**, proto3, package
`wama.measurement_session.v1`. Canonical file:
[`schema/measurement_session.proto`](schema/measurement_session.proto).
Serialization is raw Protobuf on `MeasurementSession`, keyed by canonical
`session_id`; the Kafka record timestamp equals `requested_at` rounded down to
milliseconds.

The request contains a canonical UUID, requested/start/end timestamps, sorted
unique MRIDs, and at most 32 sorted metadata entries. It represents the
half-open interval $[started\_at, ended\_at)$ and is bounded by environment
defaults of 32 MRIDs and 24 hours. It contains no raw samples.

The root-owned worker validates the request, streams Druid rows in timestamp and
MRID order into a typed long-form Parquet artifact, stores it under
`sessions/<session-id>/measurements.parquet`, and writes a raw-Protobuf receipt
for idempotent replay. It then publishes **`Blobmeta`**, proto3 package
`wama.blobmeta.v1`, from
[`schema/blobmeta.proto`](schema/blobmeta.proto) to the compacted `Blobmeta`
topic keyed by immutable `blob_id`.

`Blobmeta` records the request digest, session interval, copied context,
per-MRID row counts, total count, Parquet object identity, schema version, media
type, length, and SHA-256. V2 Parquet rows include the immutable `blob_id` and
`session_id`, with stable footer field IDs. `COMPLETE` requires every requested
MRID to have rows; `PARTIAL` preserves zero counts for missing MRIDs; bounded
request validation failures publish auditable `REJECTED` evidence without a
Parquet object. The `blobmeta-catalog` validates raw bytes and Kafka key,
commits immutable PostgreSQL metadata and coverage rows before its Kafka offset,
and accepts only digest-identical replays for a `blob_id`.

## Measurement session & alarm flow
- A producer submits a bounded `MeasurementSession` request with start/end
  context and MRIDs → the root-owned worker queries Druid, writes immutable v2
  Parquet to SeaweedFS, then publishes compacted `Blobmeta` evidence.
- The root-owned query indexer verifies the exact Parquet object against
  Blobmeta and registers that file in Iceberg. It commits its Kafka offset only
  after mutable registration-ledger and public read-only Trino evidence agree.
- Grafana selects `blob_id` and queries the registered Iceberg artifact through
  the read-only Trino datasource; object-key parsing and the internal writer are
  never exposed to the dashboard.
- Decision: should an alarm be sent? If yes → `Alarm` + Power-User notification.
- PostgreSQL provides direct metadata/coverage queries; selected-session Grafana
  presentation is available. File export, live lifecycle updates, cross-session
  analytics, and Kubernetes delivery remain excluded.

## Live-data retention
- The target policy is medium-term live storage followed by aggregation and
  archive after `X weeks`; raw data is deleted after `X weeks`.
- The Compose Druid datasource currently applies no retention, deletion,
  compaction, aggregation, or rollup policy. Those values remain deliberately
  undecided. The PMU Grafana dashboard queries raw valid values only; it does
  not introduce a retention or aggregation policy.
- Measurement sessions retained long-term (no six-week deletion).

## Consumer guidance (PoC)
- Kafka delivery is at-least-once → make processors **idempotent**.
- Keep raw/waveform data off Kafka; `MeasurementSession` carries only a bounded
  extraction command and `Blobmeta` carries integrity-checked object metadata.
- Always set `timestamp_mccs`; carry through field/gateway timestamps if present.
