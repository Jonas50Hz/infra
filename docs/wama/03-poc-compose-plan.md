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
- `c37-118-simulator` — profile-gated standalone C37.118 TCP simulator with up
   to 100 independent PMU listeners. It has no Kafka, Common Format, or gateway
   dependency; its large fleet tests remain manually armed and isolated.
- `processor-*` — Quixstreams pipelines (one service per processor).
- `measurement-session-processor` — root-owned scalable Kafka worker that
   queries Druid for bounded requests and writes immutable Parquet artifacts.
- `blobmeta-catalog` — compacted Blobmeta-to-PostgreSQL immutable metadata and
   per-MRID coverage materializer.
- `measurement-session-query-indexer` — root-owned Blobmeta consumer that
   verifies and registers exact v2 session Parquet files in Iceberg.
- `postgres` — persistent Blobmeta catalog plus a prepared future target for
   compacted Masterdata/Schema records.
- `trino` — host-exposed read-only federation coordinator; internal-only
   `trino-session-writer` plus one-shot `trino-session-init` own Iceberg table
   initialization and exact-file registration.
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

### Measurement session materialization (available now)
4. **Measurement Session Processor** — root-owned persistent workers consume
   raw-Protobuf requests, query Druid's no-rollup history, write typed Parquet
   artifacts and replay receipts to SeaweedFS, then publish compacted Blobmeta.
5. **Blobmeta catalog** — commits immutable metadata and normalized MRID
   coverage rows in PostgreSQL before each Kafka offset.
6. **Query registration and request-flow test** — the persistent indexer
   validates Blobmeta evidence before exact-file Iceberg registration. A
   profile-gated verifier submits complete and partial requests, independently
   validates Kafka, PostgreSQL, SeaweedFS, Parquet, the registration ledger,
   public Trino, and Grafana's Trino datasource.

### Metadata persistence (available now)
- **PostgreSQL** — Kafka remains the source of truth. `blobmeta-catalog` owns
  its immutable catalog schema; a future Kafka Connect mirror of `Masterdata`
  and `Schema` remains separate and unimplemented.

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
7. **SeaweedFS** — the measurement-session worker writes Parquet artifacts and
   raw replay receipts to blob; `Blobmeta` publishes only integrity metadata.
   Keep raw values off Kafka.
8. **Kafka Connect -> PostgreSQL** — mirror compacted Masterdata/Schema later;
   Blobmeta is materialized by the root-owned catalog now.
9. **Grafana data dashboards and read-only federation** — the PMU live dashboard
   over Druid and the selected-session dashboard over read-only Trino are
   available now. Trino federates Druid, PostgreSQL Blobmeta metadata, and
   registered Iceberg session files. Alerting and broader cross-session analytics
   remain deferred. This is separate from the already-provisioned VictoriaMetrics
   infrastructure dashboards.

### Phase 3 — Export (IEC 104 available now)
10. **IEC 104 exporter** — root-owned controlled station consumes typed
   raw-Protobuf `ExportRecord` values from `Export` and sends `M_SP_NA_1`,
   `M_DP_NA_1`, and `M_ME_NC_1` monitor values to one connected control center.
   The on-demand browser is the read-only live control center while a page is
   open and discards messages when its final page closes. The profile-gated
   receiver proves the exclusive wire path using only `STARTDT` and unique
   fixtures. `processor-frequency-iec104-export` now provides the direct
   configured fake-PMU frequency-to-`M_ME_NC_1` PoC producer through its own
   Forgejo repository. `processor-lfr-frequency-provision` separately seeds the
   first multi-PMU per-second selection core; complete PMU-status evidence, IEC
   104 LFR export, file/xlsx/csv export, and MQTT export remain future work.

### Phase 4 — Onboarding + config as data
11. **C37.118 Masterdata via Git (available now)** — the private
   `gateway-c37-118-onboarding` repository holds stable source location,
   literal endpoint/port, PMU IDCODE, legacy wire version 2, and immutable
   signal-to-MRID mappings. Its one-shot publisher projects reviewed
   raw-Protobuf records to compacted `Masterdata`; deleting a source emits its
   tombstone and removes only its matching managed adapter.
12. **Config & deploy flow** — config change -> Git -> automated catalog,
   contract, and deployment-guard tests -> Systemexperte decision -> audited
   Masterdata and source-gateway reconciliation. The guarded adapter path is
   limited to reviewed legacy-v2 C37.118 TCP sources.

### Phase 5 — CI/CD loop (automates deploy)
13. **Forgejo + Actions runner + registry** — infrastructure provisions
   private seeded `processor-frequency-scale`, `processor-apparent-power`, and
   `processor-frequency-iec104-export`, and
   `processor-lfr-frequency-provision`, plus the explicit
   `gateway-c37-118-onboarding` test repository and ten repository-scoped
   runner connections on one daemon. Each processor seed owns CI: test + build
   image + push for exactly one processor. The gateway-onboarding seed owns
   catalog validation, a one-shot Masterdata publisher, and source-scoped v2
   adapters generated from its reviewed catalog. The LFR seed currently
   publishes its configured preferred frequency to `LiveMeasurement`; its IEC
   104 export stage remains deferred.
14. **CD trigger** — processor jobs synchronize only their checkout to their
   isolated deployment root and run their one-service `docker compose up -d`.
   The onboarding job synchronizes only to its separately marked root, runs
   `masterdata-publisher` once, then reconciles only generated adapters for
   active source IDs. Neither path deploys or modifies the root infrastructure
   Compose project.

### Phase 6 — End-to-end pilot
15. Run one real source through the whole path:
    onboarding -> config/CI -> live data -> Druid -> Grafana -> export.

## Consumer requirements
- Processors **idempotent** (Kafka at-least-once delivery).
- Keep raw/waveform data off Kafka (blob path + `Blobmeta` pointer only).
- Value meaning resolved via Master Data config, not hard-coded; `uint_value`
  enums bound via ValueToAlias at engineering time.

## Measurement session & alarm path (Phase 2+)
Bounded raw-Protobuf `MeasurementSession` request -> Druid historical query ->
immutable v2 Parquet + receipt in SeaweedFS -> compacted raw-Protobuf `Blobmeta`
-> immutable PostgreSQL metadata/coverage catalog -> verified exact-file Iceberg
registration -> read-only Trino/Grafana selected-session query. The worker pool
scales over the 12 `MeasurementSession` partitions; `Blobmeta` also has 12
partitions. Live lifecycle updates, alarm integration, file export, and
cross-session analytics remain excluded from this slice.

## Open risks to validate early
- Quixstreams throughput / heavy waveform at target load (unproven).
- Whether a JVM engine (Flink/Beam) is needed for heavy signal processing.
- Druid single-server throughput, production topology, and retention/aggregation
   policy; alerting path (PagerDuty later?); how Trino federation expands beyond
   the current read-only Druid and Blobmeta slice.
- Whether Apache Spark is needed for future batch or heavy workloads.
