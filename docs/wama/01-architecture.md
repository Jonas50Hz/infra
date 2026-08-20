# WAMA Architecture & Technology Choices

Sources: **WAMA Platform Concept** (Gerbrand Jonas), **WAMA Platform** deck,
and the [architecture image](image.png). The image is authoritative for target
component vocabulary; the Compose PoC retains its documented Kafka KRaft and
raw-Protobuf substitutions.

## Reference architecture
The [architecture image](image.png) is the canonical target component model.
It describes source-specific gateways, stream processing, session export,
storage, visualisation, and delivery as one platform. Its target deployment
path uses Git -> CI/CD -> infrastructure Git -> ArgoCD; this PoC replaces that
path with Forgejo Actions and application-local `docker compose up -d`.

## Technology choice matrix (this plan's target)
| Layer | Choice | Role |
|-------|--------|------|
| Transport & format | Kafka (Strimzi) + Common Format | Stream backbone; one schema for all sources |
| Config & delivery | Git + Forgejo + ArgoCD | Versioned config/logic; CI tests; GitOps deploy |
| Stream processing | Quixstreams + Measurement Session Exporter | Power-User pipelines, sessions, and derived values |
| Time-series store | Druid | Live + historical query on Common Format |
| Operational observability | Grafana + VictoriaMetrics | Host and container health metrics and dashboards |
| Raw / waveform store | SeaweedFS | Blob for raw samples, waveforms, and measurement sessions |
| Config store | PostgreSQL (Kafka Connector planned) | Future Masterdata / Schema / Blobmeta mirror |
| Visualisation | Grafana (+ Trino later) | Dashboards; federated query later |
| Export | IEC 104 + file export service | Real-time and batch/file export |

## Architecture at a glance
- **Sources** (PMU, PQM, Faultrecorder, ...) → one **Gateway** container per source.
- **Kafka** is the backbone. Stream topics: `LiveMeasurement`,
  `MeasurementSession`, `Alarm`, `Export`. Compacted topics: `Masterdata`,
  `Schema`, `Blobmeta` (single source of truth).
- **PostgreSQL** holds the immutable `measurement_session_catalog` projection;
  Kafka remains the source of truth. A future Kafka Connector may mirror the
  compacted topics into it.
- **Quixstreams** processors read the stream, write derived values back to
  Kafka, and emit their configured records.
- **Finalized-session services** export a static final-only fixture, materialize
  the raw-Protobuf topic into PostgreSQL, and serve a credential-free browser
  through an anonymous read-only API.
- **Druid** directly ingests raw-Protobuf `LiveMeasurement` records from Kafka
  for live query through its Router API.
- **SeaweedFS** holds raw measurements and long-term measurement sessions (off
  Kafka).
- **VictoriaMetrics** directly collects host, Docker-container, Kafka
  broker/topic metadata, and monitoring-service telemetry for operational
  health.
- **Grafana** uses VictoriaMetrics for operational dashboards and its provisioned
  Druid datasource for live PMU measurement trends; **Trino** remains later for
  federated query.
- **Export:** root-owned `iec104-exporter` sends supported real-time monitor
  ASDUs from typed `Export` records to one control center. File (xlsx/csv) and
  MQTT export remain later work.
- **CI/CD target:** Git → CI/CD → infrastructure Git → ArgoCD (GitOps deploy).
- **Apache Spark** is a future-only option for heavy or batch workloads; it is
  not a current PoC service.

## Common Format
Every source is mapped to **`MCCSMeasurementValue`** (proto3, `rtd_schema.v1`) —
the same schema as MCCS. See [data flow](02-dataflow-contracts.md) and
[schema/rtd_schema.proto](schema/rtd_schema.proto). Serialized as raw Protobuf;
no Confluent Schema Registry. PoC uses its own MRIDs first.

## Component notes
### Kafka + Common Format
- Stream transport for measurements, derived values, measurement sessions, and
  alarms.
- Compacted topics hold config, schema, masterdata (single source of truth).
- Common Format maps every source to one schema; comparable quantities
  regardless of protocol. Raw data kept OFF Kafka on a separate path.

### Git + Forgejo + ArgoCD (target)
- Git = single source of truth for config, schema, masterdata, processing logic.
- Forgejo Actions run CI: test + containerize.
- Approved configuration moves through infrastructure Git; ArgoCD watches it
  and auto-deploys to Kubernetes.
- Onboarding a source is automatic once masterdata is created.

### Compose PoC delivery
- The infrastructure repository provisions Forgejo, Kafka, and the external
  `wama-infra` Docker network. It is never pushed to Forgejo.
- `forgejo-repos/processor-frequency-scale/` and
  `forgejo-repos/processor-apparent-power/` are separate processor repository
  seeds. `forgejo-init` creates their private Forgejo repositories, seeds each
  `main` only when it is empty, and registers separate CI and deployment runner
  connections for each.
- Processor pull requests validate only their own code and local tooling. A
  trusted processor `main` push publishes only that processor OCI image and
  deploys only its one Compose service from an isolated deployment root.
- Processor authors edit Python/Quixstreams code in their processor repository;
  YAML is optional service configuration, not a substitute for the pipeline.
- `iec104-exporter` and its profile-gated `iec104-receiver` control-center test
  are root-owned infrastructure services. A future processor may produce typed
  `Export` records but must not deploy or take ownership of either service.
- The finalized-session exporter, catalog API, browser, and contract-to-download
  Forgejo processor deployment scope. Forgejo does not deploy or modify them.
  root-owned. Root Compose builds and deploys the local Druid image; the
  processor-only Forgejo workflow neither publishes nor deploys it.
  local PoC. It is not a production isolation model.

### Quixstreams
- Runs on the measurement stream; derived values are written back to Kafka.
- Processor detection and lifecycle updates remain future work. This PoC uses a
  static final-only exporter that writes an immutable `MeasurementSession`
  record and long-term blob manifest.
- The planned [LFR per-second frequency provision](04-lfr-frequency-provision.md)
  is a separate Kafka-first processor specification. It does not extend the
  current Hz-to-mHz `processor-frequency-scale` example.
- Commit → CI tests → auto-deploy if it passes.
- **Open risk:** throughput / heavy waveform at target load unproven — benchmark
  early. Heavy signal-processing may need a JVM engine (Flink/Beam).

### Druid (time-series)
- The root-owned single-server PoC service uses Druid's Kafka and Protobuf
  extensions to decode raw `MCCSMeasurementValue` records directly from
  `LiveMeasurement`. Its image builds the descriptor from the canonical schema;
  no Schema Registry is used.
- `druid-init` creates or updates the `live_measurements` supervisor without
  resetting offsets. Kafka record time becomes `__time`, matching the Common
  Format `timestamp_mccs` contract, and individual scalar `oneof` fields remain
  queryable.
- `live_measurements` explicitly disables rollup. Druid metadata and segments
  persist in the root `druid-data` volume. Only the Router is host-exposed on
  port 8888; no Kafka ZooKeeper service or additional Druid Compose service is
  introduced.
- Retention, deletion, compaction, and aggregation policy are deliberately
  unset. Grafana provisions a pinned Druid datasource plugin and the
  `WAMA Measurements / WAMA PMU Live Measurements` dashboard for valid PMU
  voltage, current, frequency, and ROCOF trends; alerting remains deferred.

### IEC 104 export
- `iec104-exporter` is a c104-backed controlled-station server that consumes
  raw-Protobuf `ExportRecord` values from Kafka's `Export` topic.
- It sends only `M_SP_NA_1`, `M_DP_NA_1`, and `M_ME_NC_1` monitor-direction
  values to one active control center on TCP port 2404. Application commands,
  interrogation, and parameterization are rejected at a TCP ingress guard before
  c104 sees an application I-frame. Only well-formed U/S transport control
  frames reach c104. TLS/IEC 62351 and file/MQTT export remain outside this PoC
  slice.
- `iec104-receiver` is a profile-gated test control center. It starts IEC data
  transfer without general interrogation and proves the Kafka-to-ASDU path. It
  then deliberately probes a raw general-interrogation I-frame and requires the
  exporter to close the connection. It is not a production control-center
  deployment.
- `iec104-browser` is an on-demand, read-only control center. Its WebSocket page
  starts the IEC connection only while at least one browser tab is open, shows
  wire-received monitor values, and discards them when the final tab closes. It
  is mutually exclusive with the profile-gated receiver because the exporter
  permits one control center.

### VictoriaMetrics + Grafana (operational observability)
- VictoriaMetrics scrapes host and Docker-container metrics directly in the
  Compose PoC, plus Kafka broker/topic/partition/offset metadata through
  `kafka-exporter`; Grafana provisions infrastructure dashboards over it.
- This is operational telemetry only. `MCCSMeasurementValue` records, raw
  waveforms, measurement sessions, alarms, and Kafka message payloads do not enter
  VictoriaMetrics.
- Druid data is queryable through the Router and Grafana's PMU dashboard now.
  Grafana adds further measurement dashboards, alerting, and later Trino
  separately.

### SeaweedFS (raw/blob)
- S3-compatible object store for raw/waveform (PMU samples now; COMTRADE/WMU later)
  and long-term measurement sessions. The PoC uses `wama-raw` and
  `wama-measurement-sessions` buckets. Cheap, good small-file performance.

## Open decisions (from this plan)
- Druid single-server throughput and its production-scale topology.
- Druid retention, deletion, compaction, and aggregation policy.
- Alerting path: internal to start; PagerDuty possible later.
- When Trino / federated query lands.
- Stream-processing engine for heavy jobs (Quixstreams vs Beam/Flink).
- Whether Apache Spark is needed for future batch or heavy workloads.
