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
  derived values, and write them back to Kafka. They also emit
  `MeasurementSession`, `Alarm`, and `Export` records. The planned
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
  VictoriaMetrics. Trino federation and Grafana alerting remain later work.
6. **Export.** IEC 104 exporter (real-time) and File Export (xlsx/csv, on a
   configured measurement session or manual selection). MQTT exporter for
   OT/EAS.

## Kafka topics
| Topic | Type | Contents |
|-------|------|----------|
| `LiveMeasurement` | stream | `MCCSMeasurementValue` (Common Format) + derived values |
| `MeasurementSession` | stream | Bounded measurement sessions (start/end/measurement context) |
| `Alarm` | stream | Alarm records raised from measurement sessions |
| `Export` | stream | Typed raw-Protobuf `ExportRecord` values for one-way IEC 104 export; file/MQTT payloads remain future work |
| `Masterdata` | compacted | Source masterdata (IP + location, capabilities) |
| `Schema` | compacted | Common-Format schema definitions |
| `Blobmeta` | compacted | Pointers/metadata for blobs in SeaweedFS |

PostgreSQL receives the immutable `measurement_session_catalog` projection from
the `MeasurementSession` topic. Kafka remains the source of truth. A future
Kafka Connector may also mirror the compacted topics into PostgreSQL; it is not
part of this PoC.

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

## Finalized MeasurementSession contract
**`MeasurementSession`**, proto3, package `wama.measurement_session.v1`.
Canonical file: [`schema/measurement_session.proto`](schema/measurement_session.proto).
Serialization is **raw Protobuf** on the existing `MeasurementSession` topic.
It is a final-only immutable record, not a live lifecycle-update protocol.

The bounded record contains a canonical UUID session ID, source MRID, ordered
start/end/finalization timestamps, aggregate measurement/artifact counts, and
at most 32 sorted metadata entries. It contains no waveform or raw samples.

Its `ManifestReference` identifies a canonical JSON manifest in
`wama-measurement-sessions` by safe object key, byte length, media type, and a
32-byte SHA-256 digest. The manifest lists immutable artifact IDs, object keys,
media types, byte lengths, and digests. The exporter uploads the objects and
manifest before publishing Kafka; replay accepts only digest-identical content.

The catalog API revalidates the Protobuf payload and Kafka key, commits its
immutable PostgreSQL projection before its Kafka offset, and accepts only
identical replays for a session ID. It refetches and hashes the manifest for
detail/download requests, verifies SeaweedFS object length and `sha256`
metadata, then streams the selected artifact. It never returns a direct object
store URL, presigned URL, or S3 credential.

For this PoC, the `waveform` artifact is a `text/csv` series. Before export and
before browser exposure, its rows must equal `measurement_count`, have strictly
increasing timezone-aware timestamps, and span the exact session start and end.
An immutable legacy record that fails this completeness check remains retained
as evidence but is neither listed nor downloadable through the browser/API.

## Measurement session & alarm flow
- A `MeasurementSession` is recognised (or a Störschrieb is processed) and
  finalized with start/end context and aggregate counts → written as raw
  Protobuf to `MeasurementSession` + immutable long-term blobs and manifest.
- Decision: should an alarm be sent? If yes → `Alarm` + Power-User notification.
- Measurement-session data is viewable and downloadable through the anonymous
  read-only browser/API path in this trusted PoC.
- The PoC fixture exporter emits final records only. Live lifecycle updates,
  measurement analytics dashboards, and Kubernetes delivery remain excluded.

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
- Keep raw/waveform data off Kafka; a `MeasurementSession` carries only the
  integrity-checked manifest reference.
- Always set `timestamp_mccs`; carry through field/gateway timestamps if present.
