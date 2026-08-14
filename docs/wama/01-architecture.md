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
- **PostgreSQL** is provisioned as an empty persistent target. A future Kafka
  Connector mirrors the compacted topics into it.
- **Quixstreams** block: Measurement Session Exporter + Processor 1..N read the
  stream, write derived values back to Kafka, and emit measurement-session,
  alarm, and export records.
- **Druid** ingests from Kafka for live + historical query.
- **SeaweedFS** holds raw measurements and long-term measurement sessions (off
  Kafka).
- **VictoriaMetrics** directly collects host, Docker-container, Kafka
  broker/topic metadata, and monitoring-service telemetry for operational
  health.
- **Grafana** uses VictoriaMetrics for operational dashboards now, then Druid
  (+ **Trino** later for federated query) for Common Format data dashboards.
- **Export:** IEC 104 exporter (real-time) + File Export (xlsx/csv). MQTT
  exporter for OT/EAS targets.
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
- `forgejo-repos/wama-applications/` is a separate application repository seed.
  `forgejo-init` creates its empty Forgejo repository and scopes a runner to it.
- Application pull requests validate only processor code and app-local tooling.
  Trusted application `main` pushes publish only processor OCI images and deploy
  only app Compose services from an application deployment root.
- Processor authors edit Python/Quixstreams code in the application repository;
  YAML is optional service configuration, not a substitute for the pipeline.
- The planned Measurement Session Exporter belongs to this application boundary,
  not the infrastructure Compose assembly.
- The application deployment runner has host Docker access by design for this
  local PoC. It is not a production isolation model.

### Quixstreams
- Power Users (electrical engineers) write processing/detection pipelines in Python.
- Runs on the measurement stream; derived values are written back to Kafka.
- Detects and bounds `MeasurementSession` records (start, end, and measurement
  context) → `MeasurementSession` topic + long-term blob.
- Commit → CI tests → auto-deploy if it passes.
- **Open risk:** throughput / heavy waveform at target load unproven — benchmark
  early. Heavy signal-processing may need a JVM engine (Flink/Beam).

### Druid (time-series)
- Native Kafka supervisor = query-on-arrival. Auto-aggregation after ~6 weeks.
- Open decision: Druid vs ClickHouse (ClickHouse = simpler fallback).

### VictoriaMetrics + Grafana (operational observability)
- VictoriaMetrics scrapes host and Docker-container metrics directly in the
  Compose PoC, plus Kafka broker/topic/partition/offset metadata through
  `kafka-exporter`; Grafana provisions infrastructure dashboards over it.
- This is operational telemetry only. `MCCSMeasurementValue` records, raw
  waveforms, measurement sessions, alarms, and Kafka message payloads do not enter
  VictoriaMetrics.
- Grafana adds Druid (and later Trino) separately when measurement data becomes
  queryable.

### SeaweedFS (raw/blob)
- S3-compatible object store for raw/waveform (PMU samples now; COMTRADE/WMU later)
  and long-term measurement sessions. The PoC uses `wama-raw` and
  `wama-measurement-sessions` buckets. Cheap, good small-file performance.

## Open decisions (from this plan)
- Druid vs ClickHouse for time-series.
- Alerting path: internal to start; PagerDuty possible later.
- When Trino / federated query lands.
- Stream-processing engine for heavy jobs (Quixstreams vs Beam/Flink).
- Whether Apache Spark is needed for future batch or heavy workloads.
