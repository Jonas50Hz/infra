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
  `MeasurementSession` requests; live processors may emit compacted `Alarm`
  desired state and `Export` records. The planned
  [LFR per-second frequency provision](../reference/lfr-frequency-provision.md) processor
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
  queryable now. Grafana's generated `WAMA Gateways` dashboards query Druid for
  catalog-defined gateway trends and retain each record's explicit quality
  state. This is separate from the Compose PoC's
  Grafana-over-VictoriaMetrics infrastructure dashboards. Kafka exporter sends
  only operational broker/topic metadata there; no measurement, waveform,
  measurement-session, alarm, or Kafka message records are sent to
  VictoriaMetrics. Trino provides read-only federation over Druid, the
  PostgreSQL Blobmeta projection, and registered immutable session artifacts in
  Iceberg. Grafana's selected-session dashboard queries that read-only Trino
  path; its MRID selector and session link open a local confirmation UI that
  submits only the selected interval and identifiers. Its session dashboard can
  download the current immutable selection as CSV through a loopback-only,
  fixed-query Trino exporter. Grafana alerting and broader cross-session
  analytics remain later work; root-owned Alarm incident management is a
  separate Alerta/Mailpit path.
    The root-owned gateway dashboard provisioner independently replays compacted
    `Masterdata` to maintain Grafana's active-source fleet and Druid-backed
    source pages; it does not use Trino or imply gateway deployment health.
6. **Export.** IEC 104 exporter (real-time) and a Grafana-selected immutable
  session CSV download. XLSX, configured/manual broad file export, and MQTT
  exporter for OT/EAS remain future work.

## Kafka topics
| Topic | Type | Contents |
|-------|------|----------|
| `LiveMeasurement` | stream | `MCCSMeasurementValue` (Common Format) + derived values |
| `MeasurementSession` | stream | Raw-Protobuf bounded historical extraction requests |
| `Alarm` | compacted | Raw-Protobuf current active `AlarmDesiredState` values; same-key tombstones clear them |
| `AlarmEvaluationWatermark` | compacted | Raw-Protobuf latest qualifying evaluation time keyed exactly like `Alarm` |
| `Export` | stream | Typed raw-Protobuf `ExportRecord` values for one-way IEC 104 export; file/MQTT payloads remain future work |
| `Masterdata` | compacted | Raw-Protobuf `SourceMasterdata` source endpoint, location, PMU identity, and signal-to-MRID mappings |
| `Schema` | compacted | Common-Format schema definitions |
| `Blobmeta` | compacted | Raw-Protobuf immutable Parquet pointers, status, and MRID coverage |

PostgreSQL receives the immutable `blobmeta_catalog` projection from the
compacted `Blobmeta` topic. Kafka remains the source of truth. A future Kafka
Connector may mirror `Masterdata` and `Schema` separately; it is not part of
this PoC.

## Alarm desired-state contract
**`AlarmDesiredState`**, proto3 package `wama.alarm.v1`. Canonical file:
[`schema/alarm.proto`](schema/alarm.proto). `Alarm` is a root-owned, compacted
desired-active-state topic, not a MeasurementSession event stream or an
append-only lifecycle/audit topic. Live processors emit its raw-Protobuf values
when their current evaluation says an alarm is active.

A non-null raw-Protobuf value means the alarm is active. A null-valued Kafka
tombstone with the same key automatically clears and removes that active state.
For a non-null record, the Kafka key must byte-for-byte equal the payload's
UTF-8 `alarm_key`. It is exactly this unambiguous deterministic encoding of the
stable `(rule_id, exact_mrid)` pair:

`alarm/v1/<base64url-no-padding(UTF-8(rule_id))>/<base64url-no-padding(UTF-8(mrid))>`.

The slash separator cannot occur in either base64url component, so consumers
can recover both exact UTF-8 identities without ambiguity. `rule_id` and
`mrid` must be non-empty, and the payload retains both values alongside the
key.

Every active value carries an immutable canonical `episode_id`, stable rule ID,
exact MRID, `WARNING` or `CRITICAL` severity, original activation timestamp,
current rule revision, and current evidence. Evidence or rule-revision refreshes
update the same key and retain the same `episode_id`; only a later activation
after a tombstone receives a new episode ID. The contract deliberately excludes
acknowledgement and notification fields. Those workflows, and durable audit
history, belong outside `Alarm`; see [ADR 0001](../adr/0001-compacted-alarm-desired-state.md).

## Alarm evaluation watermark contract
**`AlarmEvaluationWatermark`**, proto3 package `wama.alarm.v1`. Canonical file:
[`schema/alarm.proto`](schema/alarm.proto). This root-owned compacted topic uses
the exact `Alarm` key and records the latest qualifying `last_evaluated_at` for
the same `(rule_id, exact_mrid)` identity. It is current evaluation state, not
an audit, episode, notification, or acknowledgement ledger.

The watermark remains when an inactive alarm's `Alarm` tombstone later compacts
away. A processor can therefore reject an older late measurement after restart
instead of allowing it to reactivate the cleared alarm.

### Cutover paths
#### Legacy delete-retained Alarm
A nonempty legacy `delete` `Alarm` requires the exact
`WAMA_ALARM_LEGACY_MIGRATION=discard-delete-retained-alarm-v1` guard. The
initializer deletes and recreates `Alarm` as compacted state, deliberately
discarding its retained active state without claiming recovery. It then creates
or verifies `AlarmEvaluationWatermark` normally.

#### Forward-only compact active state
When an existing compact `Alarm` retains records and the watermark topic is
absent, Kafka initialisation rejects the topology unless
`WAMA_ALARM_EVALUATION_WATERMARK_MIGRATION=accept-forward-only-alarm-evaluation-watermark-v1`.
That exact one-time guard creates and verifies only
`AlarmEvaluationWatermark`, preserving `Alarm`'s topic ID, retained bytes, and
end offsets. It does not alter or backfill `Alarm`. Recovery rejects a
pre-watermark active `Alarm` unless the guard is set; with the guard, it
bootstraps only from that active record's current evidence. The cutover is
forward-only because it establishes the late-data correctness boundary for
subsequent evaluations without claiming historical reconstruction. See
[ADR 0002](../adr/0002-alarm-evaluation-watermark.md).

## Alarm incident management
`alarm-alerta-ingress` is the sole root-owned direct consumer for this Alerta
slice. It generates its local bindings from the canonical `alarm.proto`, manually
assigns every `Alarm` partition, captures end offsets, folds a complete snapshot
of non-null active values and same-key tombstones, reconciles remote state, and
only then becomes ready. It tails the same assignment idempotently; consumer
group offsets are never its restart reconciliation source.

The ingress maps an active state to Alerta resource `MRID`, event
`wama-alarm/<episode_id>`, environment `WAMA`, customer `wama`, and fixed native
severity `indeterminate`. The domain severity appears in visible
`[WAMA WARNING]` or `[WAMA CRITICAL]` text and WAMA attributes. Native severity
never changes for an active episode, which preserves a product-side Alerta
acknowledgement across evidence and rule-revision refreshes.

Ingress ownership requires both tag `wama-managed` and attribute
`wama_managed_by=alarm-alerta-ingress`. Reconciliation considers only such
active or acknowledged records. A same-key tombstone closes matching records via
Alerta's native `PUT /api/alert/<id>/status` route; it never deletes an alert or
touches foreign or closed historic records.

Alerta uses its isolated PostgreSQL database. Its custom `post_receive` plugin
sends the fixed Mailpit recipient only for a first WAMA-managed active episode:
status `open`, no repeat, `duplicate_count=0`, prior severity `indeterminate`,
and exactly one initial `new` history entry. SMTP delivery is best-effort local
PoC behaviour only. No durable retry, external relay, outbox, or WAMA audit
ledger is implied.

## Masterdata source contract
**`SourceMasterdata`**, proto3 package `wama.masterdata.v1`. Canonical file:
[`schema/masterdata.proto`](schema/masterdata.proto). The Kafka key is the
exact UTF-8 `source_id`; non-null values are deterministic raw-Protobuf source
records and a null-valued record with the same key is a decommissioning
tombstone.

The private `gateway-c37-118` Git catalog is the reviewed authority.
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
C37.118 gateway deployment then renders one isolated adapter per active source and
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
permits one active control-center connection only and sends only
monitor-direction ASDUs while processing only TCP/IEC transport control traffic
inbound. Its TCP ingress guard
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
application response. `iec104-browser` is the root-owned, read-only control
center for operator viewing: its process establishes the `STARTDT` connection at
startup and retains it for the process lifetime, including with `viewers: 0`.
Human pages and WebSocket connections are transient viewers only, so status may
be active with zero viewers; the browser retains no values after the final page
closes. It is the same single control-center slot used by the profile-gated
receiver, so stop the browser before running the receiver workflow and do not
run them concurrently. The Forgejo-owned
[`processor-frequency-iec104-export`](../../forgejo-repos/processor-frequency-iec104-export/README.md)
seed consumes configured mapped, explicitly valid PMU-frequency values from
`LiveMeasurement` and publishes deterministic raw-Protobuf `ExportRecord`
values for `M_ME_NC_1` on `Export`. It is a direct configured PoC mapping, not
the unresolved full LFR selection algorithm.

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

`measurement-session-api` is the root-owned local-PoC browser boundary for
this contract. It normalizes a Grafana selection, creates or reuses a canonical
session UUID, validates the request, and waits for Kafka acknowledgement before
returning `202 Accepted`. It has no authority to query Druid or write session
artifacts; the worker remains the sole materializer.

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
- Live processors independently evaluate live inputs and publish the current
  `AlarmDesiredState` when active, then a same-key tombstone when it clears.
  They do not use `MeasurementSession` as an alarm lifecycle stream. The root
  ingress maps desired state into Alerta's separate acknowledgement/close
  lifecycle and first-episode Mailpit notification; Kafka remains the source of
  truth for desired state, not Alerta history.
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
- Treat `Alarm` as a compacted current-state topic: apply same-key updates
  idempotently and process its tombstones as automatic clearances.
- Maintain `AlarmEvaluationWatermark` for each newer qualifying evaluation so
  the newest evaluation remains available after an `Alarm` clearance compacts.
- Keep Alarm payloads out of VictoriaMetrics. `alarm-alerta-ingress` has no
  Druid, Grafana, Trino, SeaweedFS, PostgreSQL, or Forgejo access.
- Always set `timestamp_mccs`; carry through field/gateway timestamps if present.
