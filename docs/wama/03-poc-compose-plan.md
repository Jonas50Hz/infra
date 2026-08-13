# WAMA PoC — Docker Compose Plan (full end-to-end)

Decision record for the compose-based PoC. Follows the **WAMA Platform Concept
(Gerbrand Jonas)** only. Not Nexus. Deviations from the target are intentional.

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
- `kafka-exporter` — broker availability, topic/partition/offset, replication,
  and consumer-lag metadata for operational monitoring.
- `pmu-gateway` — fake PMU gateway: reads a startup YAML fixture and continuously
   emits raw-Protobuf `MCCSMeasurementValue` records on `LiveMeasurement`.
- `processor-*` — Quixstreams pipelines (one service per processor).
- `postgres` — backing store for compacted Masterdata/Schema/Blobmeta.
- `kafka-connect` — mirrors compacted topics to PostgreSQL.
- `druid` — time-series store, Kafka ingest (or ClickHouse fallback).
- `seaweedfs` — S3-compatible blob for raw/waveform + long-term events.
- `victoria-metrics` — single-node store for operational metrics.
- `node-exporter` — host CPU, memory, filesystem, and network metrics.
- `cadvisor` — Docker-container metrics.
- `grafana` — infrastructure dashboards over VictoriaMetrics; Druid-backed
   Common Format dashboards arrive with the measurement store.
- `exporter` — IEC 104 real-time export + file export (xlsx/csv).
- `forgejo` + `forgejo-runner` — Git + CI/CD + built-in registry.

Topics: `LiveMeasurement`, `Event`, `Alarm`, `Export`; compacted `Masterdata`,
`Schema`, `Blobmeta`.

## Build order (phased)
### Phase 1 — Backbone + processor (proves the core)
1. **Kafka (KRaft, single broker)** + Kafka-UI. Create topics.
2. **Fake PMU gateway** emitting `MCCSMeasurementValue` to `LiveMeasurement`
   from a startup YAML fixture (own MRIDs; set field/gateway/MCCS timestamps).
3. **Quixstreams processor** via `docker compose up`. Validate
   consume -> derive -> write-back to Kafka.

### Infrastructure monitoring (available now)
- **VictoriaMetrics + node-exporter + cAdvisor + kafka-exporter + Grafana** —
   directly scrape host, Docker-container, and Kafka health metadata; retain it
   for one month; and provision infrastructure dashboards. This path is
   operational telemetry only; it never receives raw Protobuf or other WAMA data
   records.

### Phase 2 — Storage + visualisation (makes data queryable)
4. **Druid** with Kafka ingest on `LiveMeasurement` — query-on-arrival.
   (ClickHouse as simpler fallback — open decision.)
5. **SeaweedFS** — processors write raw/waveform + events to blob; publish only
   a `Blobmeta` pointer to Kafka. Keep raw OFF Kafka.
6. **PostgreSQL + Kafka Connect** — mirror compacted Masterdata/Schema/Blobmeta.
7. **Grafana data dashboards** — dashboards over Druid; internal alerting to
   start. This is separate from the already-provisioned VictoriaMetrics
   infrastructure dashboards.

### Phase 3 — Export (closes the data path)
8. **Exporter service** — gateway on `Export` topic -> IEC 104; batch/file
   export -> xlsx/csv on configured event or manual selection.

### Phase 4 — Onboarding + config as data
9. **Masterdata via Git** — masterdata = IP + location committed to Git;
   gateway provisioned from it into the `Masterdata` compacted topic.
10. **Config & deploy flow** — config change -> Git -> CI tests -> deploy.

### Phase 5 — CI/CD loop (automates deploy)
11. **Forgejo + Actions runner + registry** — implemented for the Compose PoC:
   pull requests validate the PMU gateway, template, and provisioned
   `processor-*` services; `main` builds and pushes OCI images to Forgejo
   Packages.
12. **CD trigger** — implemented with a repository-scoped Forgejo Actions job
   that synchronizes an approved checkout to a dedicated host deployment root
   and runs `docker compose up -d`. Watchtower is not used.

### Phase 6 — End-to-end pilot
13. Run one real source through the whole path:
    onboarding -> config/CI -> live data -> Druid -> Grafana -> export.

## Consumer requirements
- Processors **idempotent** (Kafka at-least-once delivery).
- Keep raw/waveform data off Kafka (blob path + `Blobmeta` pointer only).
- Value meaning resolved via Master Data config, not hard-coded; `uint_value`
  enums bound via ValueToAlias at engineering time.

## Event & alarm path (Phase 2+)
Processor detects event -> records start/end/measurements -> `Event` topic +
blob (long-term) -> optional `Alarm` -> Grafana notification.

## Open risks to validate early
- Quixstreams throughput / heavy waveform at target load (unproven).
- Whether a JVM engine (Flink/Beam) is needed for heavy signal processing.
- Druid vs ClickHouse; alerting path (PagerDuty later?); when Trino lands.
