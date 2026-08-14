# WAMA PoC — Docker Compose Plan (full end-to-end)

Decision record for the Compose-based PoC. It uses the WAMA Platform Concept
(Gerbrand Jonas) plus the authoritative image/BPMN process and component
vocabulary. Nexus-specific data-plane choices remain excluded; deviations from
the target are intentional.

## Objective
Prove one source end-to-end on docker-compose, mirroring this plan's data path
without Kubernetes:
onboarding -> config/CI -> live data (Common Format) -> Quixstreams processing
-> storage (Druid + SeaweedFS) -> Grafana -> export. Then close the CI/CD loop.

## Swaps vs. target (and why)
| Target (this plan) | PoC replacement | Reason |
|--------------------|-----------------|--------|
| Strimzi (Kafka operator) | Plain Kafka, KRaft mode | Strimzi is K8s-only |
| ArgoCD (GitOps CD) | Forgejo Actions `docker compose up -d` or Watchtower | ArgoCD needs a cluster |
| Gateway via GitOps/K8s | Plain `pmu-gateway` container | No operator without K8s |
| Kafka Connector -> PostgreSQL as platform service | Same, plain containers | No operators |

## Common Format
Measurements use **`MCCSMeasurementValue`** (proto3, `rtd_schema.v1`), the same
schema as MCCS. Canonical file: `docs/wama/schema/rtd_schema.proto`. Serialized
as raw Protobuf (no Confluent Schema Registry). **PoC assigns its own MRIDs
first.** Generate Python bindings from the `.proto` in gateways + processors.

## Target compose services (full PoC)
- `kafka` — Apache Kafka, KRaft, single broker.
- `kafka-ui` — topic/message inspection.
- `pmu-gateway` — fake PMU gateway: reads a startup YAML fixture and continuously
   emits raw-Protobuf `MCCSMeasurementValue` records on `LiveMeasurement`.
- `processor-*` — Quixstreams pipelines (one service per processor), including
   the planned Measurement Session Exporter.
- `postgres` — provisioned persistent, initially empty target for compacted
   Masterdata/Schema/Blobmeta records.
- `kafka-connect` — mirrors compacted topics to PostgreSQL.
- `druid` — time-series store, Kafka ingest (or ClickHouse fallback).
- `seaweedfs` — S3-compatible blob for raw/waveform + long-term measurement
   sessions.
- `victoria-metrics` — single-node store for operational metrics.
- `node-exporter` — host CPU, memory, filesystem, and network metrics.
- `cadvisor` — Docker-container metrics.
- `grafana` — infrastructure dashboards over VictoriaMetrics; Druid-backed
   Common Format dashboards arrive with the measurement store.
- `exporter` — IEC 104 real-time export + file export (xlsx/csv).
- `forgejo` + `forgejo-runner` — Git + CI/CD + built-in registry.

Topics: `LiveMeasurement`, `MeasurementSession`, `Alarm`, `Export`; compacted
`Masterdata`, `Schema`, `Blobmeta`.

## Build order (phased)
### Phase 1 — Backbone + processor (proves the core)
1. **Kafka (KRaft, single broker)** + Kafka-UI. Create topics.
2. **Fake PMU gateway** emitting `MCCSMeasurementValue` to `LiveMeasurement`
   from a startup YAML fixture (own MRIDs; set field/gateway/MCCS timestamps).
3. **Quixstreams processor** via `docker compose up`. Validate
   consume -> derive -> write-back to Kafka.

### Infrastructure monitoring (available now)
- **VictoriaMetrics + node-exporter + cAdvisor + Grafana** — directly scrape
   host and Docker-container health metrics, retain them for one month, and
   provision infrastructure dashboards. This path is operational telemetry only;
   it never receives raw Protobuf or other WAMA data records.

### Prepared persistence target (available now)
- **PostgreSQL** — persistent empty `wama` database. Kafka remains the source
   of truth; the database receives no records until Kafka Connect is added.

### Phase 2 — Storage + visualisation (makes data queryable)
4. **Measurement Session Exporter** — application-side Quixstreams workload
   detects and bounds measurement sessions, publishes `MeasurementSession`, and
   writes long-term session objects to SeaweedFS.
5. **Druid** with Kafka ingest on `LiveMeasurement` — query-on-arrival.
   (ClickHouse as simpler fallback — open decision.)
6. **SeaweedFS** — processors write raw/waveform + measurement sessions to
   blob; publish only a `Blobmeta` pointer to Kafka. Keep raw OFF Kafka.
7. **Kafka Connect -> PostgreSQL** — mirror compacted
   Masterdata/Schema/Blobmeta into the prepared database.
8. **Grafana data dashboards** — dashboards over Druid; internal alerting to
   start. This is separate from the already-provisioned VictoriaMetrics
   infrastructure dashboards.

### Phase 3 — Export (closes the data path)
9. **Exporter service** — gateway on `Export` topic -> IEC 104; batch/file
   export -> xlsx/csv on a configured measurement session or manual selection.

### Phase 4 — Onboarding + config as data
10. **Masterdata via Git** — masterdata = IP + location committed to Git;
   gateway provisioned from it into the `Masterdata` compacted topic.
11. **Config & deploy flow** — config change -> Git -> automated quality and
   security tests -> Systemexperte decision -> auditable deployment.

### Phase 5 — CI/CD loop (automates deploy)
12. **Forgejo + Actions runner + registry** — infrastructure provisions an
   empty application repository and app-scoped runner. The separate
   `forgejo-repos/wama-applications/` seed owns CI: test + build image + push.
13. **CD trigger** — an application-repository Forgejo Actions job synchronizes
   only its checkout to an application deployment root and runs application
   `docker compose up -d` on the external infrastructure network. It never
   deploys or modifies the infrastructure Compose project.

### Phase 6 — End-to-end pilot
14. Run one real source through the whole path:
    onboarding -> config/CI -> live data -> Druid -> Grafana -> export.

## Consumer requirements
- Processors **idempotent** (Kafka at-least-once delivery).
- Keep raw/waveform data off Kafka (blob path + `Blobmeta` pointer only).
- Value meaning resolved via Master Data config, not hard-coded; `uint_value`
  enums bound via ValueToAlias at engineering time.

## Measurement session & alarm path (Phase 2+)
Processor detects a measurement session -> records start/end/measurements ->
`MeasurementSession` topic + long-term blob -> optional `Alarm` -> Grafana
notification.

## Open risks to validate early
- Quixstreams throughput / heavy waveform at target load (unproven).
- Whether a JVM engine (Flink/Beam) is needed for heavy signal processing.
- Druid vs ClickHouse; alerting path (PagerDuty later?); when Trino lands.
- Whether Apache Spark is needed for future batch or heavy workloads.
