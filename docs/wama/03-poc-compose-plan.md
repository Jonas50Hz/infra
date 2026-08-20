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
- `processor-*` — Quixstreams pipelines (one service per processor).
- `measurement-session-exporter` — profile-gated static final-only fixture
   exporter for immutable MeasurementSession records and SeaweedFS manifests.
- `measurement-session-api` — Kafka-to-PostgreSQL immutable catalog and
   anonymous read-only artifact proxy.
- `measurement-session-browser` — disposable static browser with same-origin
   API proxy.
- `postgres` — persistent immutable MeasurementSession catalog plus a prepared
   future target for compacted Masterdata/Schema/Blobmeta records.
- `kafka-connect` — mirrors compacted topics to PostgreSQL.
- `druid` — persistent single-server time-series store with direct raw-Protobuf
   Kafka ingest from `LiveMeasurement`.
- `druid-init` — idempotent Druid Kafka supervisor initializer.
- `seaweedfs` — S3-compatible blob for raw/waveform + long-term measurement
   sessions.
- `victoria-metrics` — single-node store for operational metrics.
- `node-exporter` — host CPU, memory, filesystem, and network metrics.
- `cadvisor` — Docker-container metrics.
- `grafana` — infrastructure dashboards over VictoriaMetrics and the
   Druid-backed `WAMA Measurements` PMU dashboard.
- `iec104-exporter` — root-owned one-way IEC 104 controlled station consuming
   typed raw-Protobuf `Export` records.
- `iec104-receiver` — profile-gated test control center for the IEC 104 path.
- `iec104-browser` — on-demand, read-only web control center for live
   wire-received IEC 104 values.
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

### Finalized measurement session delivery (available now)
4. **Measurement Session Exporter** — root-owned one-shot fixture service writes
   immutable artifacts plus a digest-addressed manifest to SeaweedFS, then
   publishes raw-Protobuf final records to `MeasurementSession`.
5. **Catalog API + browser** — the API materializes Kafka records idempotently
   into PostgreSQL and verifies manifests/object metadata before proxying
   downloads; the browser is anonymous, read-only, and credential-free.
6. **Contract-to-download test** — a profile-gated verifier independently
   decodes Kafka evidence and proves the browser download path.

### Prepared persistence target (available now)
- **PostgreSQL** — Kafka remains the source of truth. The session API owns its
  immutable catalog schema; a future Kafka Connect mirror of compacted records
  remains separate and unimplemented.

### Live measurement storage (available now)
- **Druid + druid-init** — root-owned persistent single-server store and
  idempotent Kafka supervisor. The image compiles a descriptor from the
  canonical Common Format schema and ingests raw `LiveMeasurement` Protobuf
  directly; there is no Schema Registry or JSON translation path.
- **Router-only access** — only the Druid Router is host-exposed on port 8888.
  Druid's internal coordination remains inside its one container, so Kafka
  remains the single plain KRaft broker with no ZooKeeper Compose service.
- **No-rollup query slice** — `live_measurements` uses Kafka record time as
  `__time`, preserves typed scalar values plus quality and source timestamps,
  and explicitly disables rollup. Root readiness and targeted CI query the PMU
  frequency fixture through the Router.
- **Deferred policy** — Druid retention, deletion, compaction, aggregation, and
   alerting are intentionally not configured. The provisioned Grafana PMU
   dashboard reads valid raw values directly from Druid without changing those
   storage policies.

### Phase 2 — Storage + visualisation (makes data queryable)
7. **SeaweedFS** — processors write raw/waveform + measurement sessions to
   blob; publish only a `Blobmeta` pointer to Kafka. Keep raw OFF Kafka.
8. **Kafka Connect -> PostgreSQL** — mirror compacted
   Masterdata/Schema/Blobmeta into the prepared database.
9. **Grafana data dashboards** — the PMU live dashboard over Druid is available
   now. Additional measurement dashboards and internal alerting remain deferred.
   This is separate from the already-provisioned VictoriaMetrics infrastructure
   dashboards.

### Phase 3 — Export (IEC 104 available now)
10. **IEC 104 exporter** — root-owned controlled station consumes typed
   raw-Protobuf `ExportRecord` values from `Export` and sends `M_SP_NA_1`,
   `M_DP_NA_1`, and `M_ME_NC_1` monitor values to one connected control center.
   The on-demand browser is the read-only live control center while a page is
   open and discards messages when its final page closes. The profile-gated
   receiver proves the exclusive wire path using only `STARTDT` and unique
   fixtures. An export-producing processor, file/xlsx/csv export, and MQTT
   export remain future work.

### Phase 4 — Onboarding + config as data
11. **Masterdata via Git** — masterdata = IP + location committed to Git;
   gateway provisioned from it into the `Masterdata` compacted topic.
12. **Config & deploy flow** — config change -> Git -> automated quality and
   security tests -> Systemexperte decision -> auditable deployment.

### Phase 5 — CI/CD loop (automates deploy)
13. **Forgejo + Actions runner + registry** — infrastructure provisions
   private seeded `processor-frequency-scale` and `processor-apparent-power`
   repositories and four repository-scoped runner connections on one daemon.
   Each separate `forgejo-repos/processor-*/` seed owns CI: test + build image
   + push for exactly one processor.
14. **CD trigger** — each processor's Forgejo Actions job synchronizes only its
   checkout to its isolated deployment root and runs that processor's
   `docker compose up -d` on the external infrastructure network. It never
   deploys or modifies the infrastructure Compose project.

### Phase 6 — End-to-end pilot
15. Run one real source through the whole path:
    onboarding -> config/CI -> live data -> Druid -> Grafana -> export.

## Consumer requirements
- Processors **idempotent** (Kafka at-least-once delivery).
- Keep raw/waveform data off Kafka (blob path + `Blobmeta` pointer only).
- Value meaning resolved via Master Data config, not hard-coded; `uint_value`
  enums bound via ValueToAlias at engineering time.

## Measurement session & alarm path (Phase 2+)
Static fixture exporter finalizes a session -> immutable artifact objects +
manifest -> raw-Protobuf `MeasurementSession` topic -> PostgreSQL catalog ->
anonymous read-only browser/API download. Live lifecycle updates, alarm
integration, and analytics dashboards remain excluded from this slice.

## Open risks to validate early
- Quixstreams throughput / heavy waveform at target load (unproven).
- Whether a JVM engine (Flink/Beam) is needed for heavy signal processing.
- Druid single-server throughput, production topology, and retention/aggregation
   policy; alerting path (PagerDuty later?); when Trino lands.
- Whether Apache Spark is needed for future batch or heavy workloads.
