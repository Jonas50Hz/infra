# WAMA Data Flow (Common Format)

Source: **WAMA Platform Concept** (Gerbrand Jonas) — "Process — Live data",
"Process — Event & alarm", "Architecture at a glance". This plan only.

> NOTE: This is deliberately NOT the WAMA Nexus data flow. Nexus uses a
> Concentrator that time-aligns readings into TimeStep envelopes, Confluent
> Schema Registry, and a PMUReading/STAT contract. **None of that applies here.**
> This plan normalises every source to **Common Format** and routes it through
> the topics below.

## Path
1. **Source → Gateway.** One gateway container per source (PMU, PQM,
   Faultrecorder, ...). Gateway connects, pulls the stream, and normalises to
   Common Format (`MCCSMeasurementValue`).
2. **Gateway → Kafka.** Normalised measurements published to `LiveMeasurement`.
3. **Processing.** Quixstreams processors consume `LiveMeasurement`, compute
   derived values, and write them back to Kafka. They also emit `Event`,
   `Alarm`, and `Export` records.
4. **Storage.**
   - Druid ingests from Kafka for live + historical query.
   - Raw / waveform data goes to SeaweedFS (off Kafka). Raw is deleted after
     X weeks; aggregated live data is archived.
   - Events go to long-term storage (not deleted after 6 weeks).
5. **Visualisation.** Grafana over Druid (Trino for federated query later) for
  Common Format measurement data. This is separate from the Compose PoC's
  Grafana-over-VictoriaMetrics infrastructure dashboards. Kafka exporter sends
  only operational broker/topic metadata there; no measurement, waveform,
  event, alarm, or Kafka message records are sent to VictoriaMetrics.
6. **Export.** IEC 104 exporter (real-time) and File Export (xlsx/csv, on a
   configured event or manual selection). MQTT exporter for OT/EAS.

## Kafka topics
| Topic | Type | Contents |
|-------|------|----------|
| `LiveMeasurement` | stream | `MCCSMeasurementValue` (Common Format) + derived values |
| `Event` | stream | Detected/classified events (start/end/measurements) |
| `Alarm` | stream | Alarm records raised from events |
| `Export` | stream | Records destined for IEC 104 / file export |
| `Masterdata` | compacted | Source masterdata (IP + location, capabilities) |
| `Schema` | compacted | Common-Format schema definitions |
| `Blobmeta` | compacted | Pointers/metadata for blobs in SeaweedFS |

Compacted topics are mirrored to PostgreSQL via a Kafka Connector.

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

## Event & alarm flow
- Event recognised (or Störschrieb processed) → recorded with start point, end
  point, and measurements → written to `Event` + long-term blob.
- Decision: should an alarm be sent? If yes → `Alarm` + Power-User notification.
- Event data viewable in-system and exportable to CSV.

## Live-data retention
- All live data stored (medium-term) then aggregated after X weeks and archived.
- Raw data stored then deleted after X weeks.
- Events retained long-term (no 6-week deletion).

## Consumer guidance (PoC)
- Kafka delivery is at-least-once → make processors **idempotent**.
- Keep raw/waveform data off Kafka; publish only a `Blobmeta` pointer.
- Always set `timestamp_mccs`; carry through field/gateway timestamps if present.
