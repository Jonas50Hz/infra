# WAMA Architecture & Technology Choices

Source: **WAMA Platform Concept** (Gerbrand Jonas) + **WAMA Platform** deck.
This plan only. Not Nexus.

## Technology choice matrix (this plan's target)
| Layer | Choice | Role |
|-------|--------|------|
| Transport & format | Kafka (Strimzi) + Common Format | Event-based backbone; one schema for all sources |
| Config & delivery | Git + Forgejo + ArgoCD | Versioned config/logic; CI tests; GitOps deploy |
| Stream processing | Quixstreams | Power-User pipelines; detection & derived values |
| Time-series store | Druid | Live + historical query on Common Format |
| Raw / waveform store | SeaweedFS | Blob for raw samples, waveforms, events |
| Config store | PostgreSQL (via Kafka Connector) | Masterdata / Schema / Blobmeta backing store |
| Visualisation | Grafana (+ Trino later) | Dashboards; federated query later |
| Export | IEC 104 + file export service | Real-time and batch/file export |

## Architecture at a glance
- **Sources** (PMU, PQM, Faultrecorder, ...) → one **Gateway** container per source.
- **Kafka** is the backbone. Topics: `LiveMeasurement`, `Event`, `Alarm`, `Export`.
  Compacted topics: `Masterdata`, `Schema`, `Blobmeta` (single source of truth).
- **PostgreSQL** mirrors the compacted topics via a Kafka Connector.
- **Quixstreams** block: Event Exporter + Processor 1..N read the stream, write
  derived values back to Kafka, emit events/alarms/export records.
- **Druid** ingests from Kafka for live + historical query.
- **SeaweedFS** holds raw measurements + long-term events (off Kafka).
- **Grafana** (+ **Trino** later for federated query) for dashboards.
- **Export:** IEC 104 exporter (real-time) + File Export (xlsx/csv). MQTT
  exporter for OT/EAS targets.
- **CI/CD:** Forgejo/Git → Infra Git + ArgoCD (GitOps deploy).

## Common Format
Every source is mapped to **`MCCSMeasurementValue`** (proto3, `rtd_schema.v1`) —
the same schema as MCCS. See [data flow](02-dataflow-contracts.md) and
[schema/rtd_schema.proto](schema/rtd_schema.proto). Serialized as raw Protobuf;
no Confluent Schema Registry. PoC uses its own MRIDs first.

## Component notes
### Kafka (Strimzi) + Common Format
- Event-based transport for measurements, derived values, events, alarms.
- Compacted topics hold config, schema, masterdata (single source of truth).
- Common Format maps every source to one schema; comparable quantities
  regardless of protocol. Raw data kept OFF Kafka on a separate path.

### Git + Forgejo + ArgoCD
- Git = single source of truth for config, schema, masterdata, processing logic.
- Forgejo Actions run CI: test + containerize.
- ArgoCD watches Git and auto-deploys to Kubernetes.
- Onboarding a source is automatic once masterdata is created.

### Quixstreams
- Power Users (electrical engineers) write processing/detection pipelines in Python.
- Runs on the event stream; derived values written back to Kafka.
- Detects/classifies events (start/end/mrid) → event topic + blob.
- Commit → CI tests → auto-deploy if it passes.
- **Open risk:** throughput / heavy waveform at target load unproven — benchmark
  early. Heavy signal-processing may need a JVM engine (Flink/Beam).

### Druid (time-series)
- Native Kafka supervisor = query-on-arrival. Auto-aggregation after ~6 weeks.
- Open decision: Druid vs ClickHouse (ClickHouse = simpler fallback).

### SeaweedFS (raw/blob)
- S3-compatible object store for raw/waveform (PMU samples now; COMTRADE/WMU later)
  and long-term events. Cheap, good small-file performance.

## Open decisions (from this plan)
- Druid vs ClickHouse for time-series.
- Alerting path: internal to start; PagerDuty possible later.
- When Trino / federated query lands.
- Stream-processing engine for heavy jobs (Quixstreams vs Beam/Flink).
