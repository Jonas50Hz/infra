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
  `MeasurementSession`, `Alarm`, and `Export` records.
4. **Storage.**
   - Druid ingests from Kafka for live + historical query.
   - Raw / waveform data goes to SeaweedFS (off Kafka). Raw is deleted after
     X weeks; aggregated live data is archived.
   - Measurement sessions go to long-term storage (not deleted after six
     weeks).
5. **Visualisation.** Grafana over Druid (Trino for federated query later) for
  Common Format measurement data. This is separate from the Compose PoC's
  Grafana-over-VictoriaMetrics infrastructure dashboards. Kafka exporter sends
  only operational broker/topic metadata there; no measurement, waveform,
  measurement-session, alarm, or Kafka message records are sent to
  VictoriaMetrics.
6. **Export.** IEC 104 exporter (real-time) and File Export (xlsx/csv, on a
   configured measurement session or manual selection). MQTT exporter for
   OT/EAS.

## Kafka topics
| Topic | Type | Contents |
|-------|------|----------|
| `LiveMeasurement` | stream | `MCCSMeasurementValue` (Common Format) + derived values |
| `MeasurementSession` | stream | Bounded measurement sessions (start/end/measurement context) |
| `Alarm` | stream | Alarm records raised from measurement sessions |
| `Export` | stream | Records destined for IEC 104 / file export |
| `Masterdata` | compacted | Source masterdata (IP + location, capabilities) |
| `Schema` | compacted | Common-Format schema definitions |
| `Blobmeta` | compacted | Pointers/metadata for blobs in SeaweedFS |

PostgreSQL is provisioned as an empty target. A future Kafka Connector will
mirror these compacted topics into it; Kafka remains the source of truth until
then.

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

## Measurement session & alarm flow
- A `MeasurementSession` is recognised (or a Störschrieb is processed) and
  recorded with start point, end point, and measurements → written to
  `MeasurementSession` + long-term blob.
- Decision: should an alarm be sent? If yes → `Alarm` + Power-User notification.
- Measurement-session data is viewable in-system and exportable to CSV.
- `MeasurementSession` is a lifecycle record, not another
  `MCCSMeasurementValue`; its payload schema is a follow-up contract and is not
  introduced by this PoC terminology migration.

## Live-data retention
- All live data stored (medium-term) then aggregated after X weeks and archived.
- Raw data stored then deleted after X weeks.
- Measurement sessions retained long-term (no six-week deletion).

## Consumer guidance (PoC)
- Kafka delivery is at-least-once → make processors **idempotent**.
- Keep raw/waveform data off Kafka; publish only a `Blobmeta` pointer.
- Always set `timestamp_mccs`; carry through field/gateway timestamps if present.
