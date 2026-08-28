(exp_wama_architecture)=

```{meta}
:description: WAMA architecture, technology choices, and the rationale for the local Compose proof of concept.
```

# WAMA architecture and technology choices

Sources: **WAMA Platform Concept** (Gerbrand Jonas), **WAMA Platform** deck,
and the [architecture image](../wama/image.png). The image is authoritative for target
component vocabulary; the Compose PoC retains its documented Kafka KRaft and
raw-Protobuf substitutions.

## Reference architecture
The [architecture image](../wama/image.png) is the canonical target component model.
It describes source-specific gateways, stream processing, session export,
storage, visualization, and delivery as one platform. Its target deployment
path uses Git -> CI/CD -> infrastructure Git -> ArgoCD; this PoC replaces that
path with Forgejo Actions and application-local `docker compose up -d`.

## Technology choice matrix (this plan's target)
| Layer | Choice | Role |
|-------|--------|------|
| Transport & format | Kafka (Strimzi) + Common Format | Stream backbone; one schema for all sources |
| Config & delivery | Git + Forgejo + ArgoCD | Versioned config/logic; CI tests; GitOps deploy |
| Stream processing | Quixstreams + Measurement Session Worker | Power-User pipelines, sessions, and derived values |
| Time-series store | Druid | Live + historical query on Common Format |
| Operational observability | Grafana + VictoriaMetrics | Host and container health metrics and dashboards |
| Raw / waveform store | SeaweedFS | Blob for raw samples, waveforms, and measurement sessions |
| Config store | PostgreSQL | Git-authoritative C37.118 Masterdata projected to Kafka now; root-owned Blobmeta projection in PostgreSQL; Masterdata/Schema mirror later |
| Visualization | Grafana + Trino | Dashboards and read-only federation |
| Export | IEC 104 + file export service | Real-time and batch/file export |

## Architecture at a glance
- **Sources** (PMU, PQM, Faultrecorder, ...) → one **Gateway** container per source.
- **Kafka** is the backbone. Stream topics: `LiveMeasurement`,
  `MeasurementSession`, `Export`. Compacted topics: `Alarm`, `Masterdata`,
  `AlarmEvaluationWatermark`, `Schema`, `Blobmeta` (single source of truth).
- **Masterdata** is a raw-Protobuf source catalog projection keyed by stable
  source ID. Git holds the reviewed authority; the compacted Kafka topic holds
  current runtime state and source tombstones.
- **Alarm** is a root-owned raw-Protobuf desired-active-state projection keyed
  by stable rule ID and exact MRID. Live processors publish current active
  state; a same-key tombstone clears it. `alarm-alerta-ingress` reconstructs
  that compacted state through captured end offsets and maps it to Alerta
  episodes without treating Kafka as an acknowledgement or audit stream.
- **AlarmEvaluationWatermark** is a root-owned compacted current-state topic
  with the same key as `Alarm`. It retains the newest qualifying evaluation
  time after an `Alarm` tombstone compacts away, so an older late measurement
  cannot reactivate a cleared alarm after recovery.
- **PostgreSQL** holds the immutable `blobmeta_catalog` projection. Kafka
  remains the source of truth; a future connector may mirror compacted
  `Masterdata` and `Schema` records separately.
- **Alerta PostgreSQL** is a separate durable product database. It is not the
  Blobmeta projection and has no host port.
- **Quixstreams** processors read the stream, write derived values back to
  Kafka, and emit their configured records. Live processors publish
  `AlarmDesiredState` updates and same-key clear tombstones independently of
  MeasurementSession processing.
- **Measurement-session services** consume bounded raw-Protobuf requests,
  query Druid, create immutable v2 Parquet artifacts in SeaweedFS, publish
  compacted raw-Protobuf `Blobmeta`, materialize metadata-only PostgreSQL rows,
  and register verified exact artifact files in Iceberg.
- **Druid** directly ingests raw-Protobuf `LiveMeasurement` records from Kafka
  for live query through its Router API.
- **SeaweedFS** holds raw measurements and long-term measurement sessions (off
  Kafka).
- **VictoriaMetrics** directly collects host, Docker-container, Kafka
  broker/topic metadata, and monitoring-service telemetry for operational
  health.
- **Grafana** uses VictoriaMetrics for operational dashboards, Druid for live
  PMU measurement trends, and read-only Trino for selected immutable
  measurement-session artifacts. **Trino** federates Druid, PostgreSQL Blobmeta
  metadata, and the Iceberg session table; its internal writer is not exposed to
  Grafana or the host.
- **Gateway dashboard provisioning** is root-owned: it consumes compacted
  Masterdata directly, creates a Grafana fleet page and source-scoped Druid
  views, and removes generated pages on source tombstones. It does not manage
  gateway containers or use Trino as a dashboard control plane.
- **Alarm incidents:** root-owned Alerta exposes an operator acknowledgement and
  close UI/API on loopback only. A custom `post_receive` plugin sends one
  best-effort local Mailpit email for a first active WAMA episode; refreshes,
  acknowledgements, closes, and rule-only revisions do not send email.
- **Export:** root-owned `iec104-exporter` sends supported real-time monitor
  ASDUs from typed `Export` records to one control center. The persistent,
  viewer-only `iec104-browser` owns that connection from application startup,
  including with zero viewers. Grafana can download
  a selected immutable measurement session as CSV through the loopback-only
  fixed-query exporter. XLSX, broader file export, and MQTT remain later work.
- **CI/CD target:** Git → CI/CD → infrastructure Git → ArgoCD (GitOps deploy).
- **Apache Spark** is a future-only option for heavy or batch workloads; it is
  not a current PoC service.

## Common Format
Every source is mapped to **`MCCSMeasurementValue`** (proto3, `rtd_schema.v1`) —
the same schema as MCCS. See
[WAMA data flow contracts](../reference/wama-data-flow-contracts.md) and
[schema/rtd_schema.proto](../wama/schema/rtd_schema.proto). Serialized as raw Protobuf;
no Confluent Schema Registry. PoC uses its own MRIDs first.

## Component notes
### Kafka + Common Format
- Stream transport for measurements, derived values, measurement sessions, and
  export.
- Compacted topics hold configuration, schema, masterdata, and the current
  root-owned `Alarm` active state. Its same-key tombstones are automatic
  clearances, not acknowledgement, notification, or audit records.
- Common Format maps every source to one schema; comparable quantities
  regardless of protocol. Raw data kept OFF Kafka on a separate path.

### Alarm incident management
- `alarm-alerta-ingress` consumes raw-Protobuf compacted `Alarm` directly. It
  manually folds every partition through captured end offsets, reconciles only
  tagged and attributed WAMA-managed active/ack Alerta records, then tails
  idempotently without consumer-group recovery state.
- A nonempty legacy `delete` `Alarm` requires the exact
  `WAMA_ALARM_LEGACY_MIGRATION=discard-delete-retained-alarm-v1` guard to
  discard and recreate it as compacted state. That legacy discard/recreate path
  intentionally drops retained active state and does not recover it; the
  watermark then initializes normally.
- The separate watermark cutover is deliberately forward-only. For an existing
  compact `Alarm` with retained active state and no watermark, setting
  `WAMA_ALARM_EVALUATION_WATERMARK_MIGRATION=accept-forward-only-alarm-evaluation-watermark-v1`
  creates and verifies only `AlarmEvaluationWatermark`, preserving `Alarm`'s
  topic ID, retained bytes, and end offsets. It does not rewrite or backfill
  `Alarm`; a pre-cutover active alarm can bootstrap only from its current
  evidence. This preserves the late-data boundary for future evaluations
  without claiming historical repair; see
  [ADR 0002](../adr/0002-alarm-evaluation-watermark.md).
- Each active desired state maps to resource MRID and event
  `wama-alarm/<episode-id>` in environment `WAMA`, customer `wama`, and fixed
  native severity `indeterminate`. WAMA `WARNING` or `CRITICAL` remains in the
  visible `[WAMA ...]` text and attributes so a native acknowledgement survives
  evidence and rule-revision refreshes.
- A tombstone calls Alerta's native status route to close only its matching
  ingress-owned active or acknowledged alert. The ingress neither deletes
  alerts nor changes foreign or already closed history.
- Alerta and its isolated PostgreSQL database are root infrastructure. Mailpit
  SMTP is local-PoC, best-effort notification only; it has no durable retry or
  outbox contract. Alerta uses `SECRET_KEY`, retains the no-account local policy,
  and enables both `reject` and the custom activation-mail plugin so
  `ALLOWED_ENVIRONMENTS` remains enforced. Neither service sends alarm payloads
  to VictoriaMetrics.

### Git + Forgejo + ArgoCD (target)
- Git = reviewed authority for config, schema, masterdata, and processing logic.
- Forgejo Actions run CI: test + containerize.
- Approved configuration moves through infrastructure Git; ArgoCD watches it
  and auto-deploys to Kubernetes.
- The current C37.118 gateway slice validates Git masterdata, publishes it
  to Kafka, and reconciles one isolated legacy-v2 TCP adapter per approved
  source. It remains separate from the root-owned `pmu-gateway` and simulator.

### Compose PoC delivery
- The infrastructure repository provisions Forgejo, Kafka, and the external
  `wama-infra` Docker network. It is never pushed to Forgejo.
- `forgejo-repos/processor-frequency-scale/`,
  `forgejo-repos/processor-apparent-power/`,
  `forgejo-repos/processor-frequency-iec104-export/`, and
  `forgejo-repos/processor-alarm-threshold/`, and the standard
  `processor-frequency-measurement-session` seed are separate processor
  repository seeds. `forgejo-init` creates their private Forgejo repositories,
  seeds each `main` only when it is empty, and registers twelve separate CI and
  deployment runner connections for the six managed repositories, including the
  C37.118 gateway repository.
- Processor pull requests validate only their own code and local tooling. A
  trusted processor `main` push publishes only that processor OCI image and
  deploys only its one Compose service from an isolated deployment root.
- `forgejo-repos/gateway-c37-118/` is the one explicitly declared
  gateway-deployment-test repository. Its one-shot `masterdata-publisher`
  validates C37.118 PMU catalog files and reconciles raw-Protobuf `Masterdata`
  records over the external `wama-infra` network. Its guarded deployment helper
  then renders and reconciles only source-scoped legacy-v2 adapters from that
  same approved catalog. It cannot deploy the root `pmu-gateway`, run the root
  Compose project, or control any unlisted gateway.
- Processor authors edit Python/Quixstreams code in their processor repository;
  YAML is optional service configuration, not a substitute for the pipeline.
- `iec104-exporter` and its profile-gated `iec104-receiver` control-center test
  are root-owned infrastructure services. A future processor may produce typed
  `Export` records but must not deploy or take ownership of either service.
- `measurement-session-processor`, `blobmeta-catalog`, and the profile-gated
  request-flow verifier are root-owned. Forgejo does not deploy or modify them.
  Root Compose also builds and deploys the local Druid image; the processor-only
  Forgejo workflow neither publishes nor deploys root infrastructure. This is a
  local PoC and not a production isolation model.

### Quixstreams
- Runs on the measurement stream; derived values are written back to Kafka.
- The root-owned measurement-session worker consumes `MeasurementSession`
  extraction requests, reads Druid's no-rollup historical values, and writes
  typed Parquet plus immutable `Blobmeta` evidence. It scales as a Kafka
  consumer group across the 12 request-topic partitions; Compose does not spawn
  a privileged container per request.
- The standard `processor-frequency-measurement-session` seed turns Frequency
  Capture Episodes from `LiveMeasurement` into bounded `MeasurementSession`
  requests. Its timing is PoC best effort, does not guarantee Druid visibility
  or tail completeness, and does not represent Alarm lifecycle.
- The [LFR per-second frequency provision](../reference/lfr-frequency-provision.md)
  is a future Kafka-first processor specification. A future implementation may
  publish a configured preferred-frequency value to `LiveMeasurement`; IEC 104
  export, complete PMU status evidence, and durable audit storage remain
  separate work. It does not extend the current Hz-to-mHz
  `processor-frequency-scale` example or the direct IEC 104 frequency mapping.
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
  generated `WAMA Gateways` dashboards for live source values. Grafana alerting
  remains separate from the root-owned Alarm-to-Alerta incident path.

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
- `iec104-browser` is a persistent, read-only control center. It opens its IEC
  connection at application startup and retains it with zero browser viewers.
  WebSocket pages show wire-received monitor values and are viewer-only. It is
  mutually exclusive with the profile-gated receiver because the exporter
  permits one control center; stop the browser before running that test.

### VictoriaMetrics + Grafana (operational observability)
- VictoriaMetrics scrapes host and Docker-container metrics directly in the
  Compose PoC, plus Kafka broker/topic/partition/offset metadata through
  `kafka-exporter`; Grafana provisions infrastructure dashboards over it.
- This is operational telemetry only. `MCCSMeasurementValue` records, raw
  waveforms, measurement sessions, alarms, and Kafka message payloads do not enter
  VictoriaMetrics.
- Druid data is queryable through the Router and Grafana's PMU dashboard now.
  Trino also provides read-only Druid, Blobmeta, and selected-session Iceberg
  federation. Grafana-specific alerting and broader analytic dashboards remain
  separate from the Alarm-to-Alerta path.

### SeaweedFS (raw/blob)
- S3-compatible object store for raw/waveform (PMU samples now; COMTRADE/WMU later)
  and long-term measurement sessions. The PoC uses `wama-raw` and
  `wama-measurement-sessions` buckets. Cheap, good small-file performance.

## Open decisions (from this plan)
- Druid single-server throughput and its production-scale topology.
- Druid retention, deletion, compaction, and aggregation policy.
- Reviewed production escalation beyond the local Alerta/Mailpit proof.
- How the current selected-session Trino federation evolves into broader
  cross-session analytics and query access controls.
- Stream-processing engine for heavy jobs (Quixstreams vs Beam/Flink).
- Whether Apache Spark is needed for future batch or heavy workloads.
