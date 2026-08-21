# WAMA PoC — Agent Instructions (always-on)

## What this repo is
Local **docker-compose** PoC of the WAMA data platform. Goal: test Quixstreams
processors against a Kafka backbone, then use Forgejo only for internal
processor deployment and explicitly scoped gateway-deployment tests.
This is NOT the production target (production is Kubernetes-based).

## Domain Vocabulary
- [docs/wama/image.png](../docs/wama/image.png),
  [docs/wama/User_Konfiguration_Nexus.bpmn](../docs/wama/User_Konfiguration_Nexus.bpmn),
  and [docs/wama/Livedaten_Prozess_WAMA.bpmn](../docs/wama/Livedaten_Prozess_WAMA.bpmn)
  are authoritative for target WAMA component and process vocabulary.
- `MeasurementSession` is the bounded historical extraction request with start,
  end, sorted MRIDs, and context. It replaces the overly broad `Event`
  contract; use the `MeasurementSession` Kafka topic and
  `wama-measurement-sessions` bucket. Completed immutable artifacts are
  described on compacted `Blobmeta` records.
- The diagram references do not alter the PoC data plane: retain Common Format
  `MCCSMeasurementValue`, raw Protobuf, and the plain Kafka KRaft topology.

## Hard rules (do not violate)
- **No Kubernetes artifacts.** Never generate Helm charts, K8s manifests,
  Strimzi CRDs, or ArgoCD `Application` resources. Those are the production
  stack, not this PoC.
- **Kafka = plain Apache Kafka in KRaft mode** (single broker, no ZooKeeper).
  Non-Confluent stack. No Confluent Schema Registry — use JSON or raw Protobuf;
  add Apicurio only if a registry is explicitly needed.
- **CD replacement:** ArgoCD is K8s-only. For this PoC, Forgejo Actions may
  deploy internal `processor-*` services, or a gateway only when explicitly
  testing Forgejo-based gateway deployment, using application-local
  `docker compose up -d` or Watchtower on new application image tags. Forgejo
  must never deploy, modify, or trigger deployment of the root infrastructure
  Compose project.
- `gateway-c37-118-onboarding` is the one declared gateway-deployment test.
  Its scope is one-shot publication of raw-Protobuf `Masterdata` from reviewed
  legacy C37.118 version-2 PMU catalog files plus source-scoped adapters
  rendered by its guarded, marker-owned deployment root. It may start, recreate,
  or remove only a `c37-118-gateway-<source-id>` derived from the approved
  catalog; a tombstone may remove only its matching previously managed adapter.
  It must not control the root `pmu-gateway`, simulator, or any other gateway.
- The root `docker-compose.yml` runs and deploys infrastructure services. The
  current `pmu-gateway`, all infrastructure gateways, and every asset outside
  the explicit Forgejo deployment scope belong in this repository, never in an
  application Compose assembly.
- **Infrastructure metrics = VictoriaMetrics.** Use its single-node direct
  scrape configuration for this PoC; do not add a separate Prometheus server or
  `vmagent` unless the topology changes.

## Compose service layout
- The root `docker-compose.yml` is the assembly layer: Compose `include`
  entries and genuinely shared top-level resources belong there.
- Every declared Compose service must own a directory at
  `services/<compose-service-name>/`, including one-shot helper services.
- Each service directory must contain its `compose.yaml`. Keep that service's
  Dockerfile, environment/config files, mounted scripts, health check, ports,
  dependencies, and service-specific documentation there. A service directory
  must remain tracked even when it only needs a short README.
- When adding, renaming, or removing a Compose service, make the corresponding
  directory and Compose-fragment change; then update the root `include` list,
  the root README, and these instructions in the same change.
- The current Phase 1 source gateway is `pmu-gateway`; it publishes configured
  fake PMU scalar measurements as raw `MCCSMeasurementValue` Protobuf records.
- `c37-118-simulator` owns `services/c37-118-simulator/`. It is an opt-in,
  root-owned standalone C37.118 TCP source simulator with no Kafka, Common
  Format, gateway, or Forgejo dependency. Its large fleet checks stay manually
  armed and isolated from `wama-infra`.
- Druid owns `services/druid/` and persists its single-server state in
  `druid-data`; `druid-init` owns `services/druid-init/` and must idempotently
  initialize the raw-Protobuf `LiveMeasurement` supervisor. Druid's Router is
  the only Druid API with a host mapping. Its in-container coordination does not
  add a ZooKeeper Compose service or alter Kafka KRaft.
- `infra-readiness` owns `services/infra-readiness/` and is the root stack's
  one-shot behavioral readiness gate. It must remain infrastructure-only and
  must not deploy or validate application processors.
- PostgreSQL owns `services/postgres/` and provides the persistent target for
  root-owned `blobmeta-catalog` materialization of compacted `Blobmeta`
  records. `blobmeta-catalog` may create only its immutable `blobmeta_catalog`
  schema; Kafka remains the source of truth. A future Kafka Connect mirror of
  `Masterdata` and `Schema` remains separate.
- Trino owns `services/trino/`, internal-only
  `services/trino-session-writer/`, and one-shot `services/trino-init/` plus
  `services/trino-session-init/`.
  The host-exposed coordinator is a root-owned, read-only trusted-PoC
  federation service over Druid, the PostgreSQL Blobmeta projection, and
  registered immutable session Parquet artifacts in SeaweedFS. The writer has
  no host mapping and is reserved for root-owned verified Iceberg registration;
  neither service may modify canonical session artifacts.
- SeaweedFS owns `services/seaweedfs/` and provides the PoC's authenticated
  S3-compatible raw/waveform and long-term measurement-session storage.
- Measurement-session services own `services/measurement-session-processor/`,
  `services/blobmeta-catalog/`, `services/measurement-session-query-indexer/`,
  and `services/measurement-session-e2e/`. They are root-owned, not Forgejo
  deployment targets. The processor accesses Druid and SeaweedFS to create
  Parquet artifacts; the catalog holds no S3 credentials and projects only
  Blobmeta metadata to PostgreSQL. The query indexer verifies canonical
  artifacts and registers their exact Parquet object URI through the
  internal-only Trino writer; it owns only mutable query-registration state.
- IEC 104 services own `services/iec104-exporter/`,
  `services/iec104-receiver/`, and `services/iec104-browser/`. The exporter is
  a root-owned one-way controlled station that consumes typed raw-Protobuf
  `Export` records; the receiver is a profile-gated control-center test only;
  the browser is an on-demand, read-only control center that holds no values
  after its final page closes. None is a Forgejo deployment target. The
  independent `processor-frequency-iec104-export` seed is the direct configured
  PMU-frequency-to-`M_ME_NC_1` producer; it is not the unresolved full LFR
  preferred-frequency algorithm.
- `processor-lfr-frequency-provision` is the separate Forgejo-owned per-second
  LFR core. It publishes a configured preferred-frequency Common Format value
  to `LiveMeasurement`; it does not deploy root IEC 104 services or produce
  `Export` records until that later contract increment is explicitly scoped.
- Forgejo services own `services/forgejo/`, `services/forgejo-init/`, and
  `services/forgejo-runner/`.
- This repository is never pushed to Forgejo. Each tracked seed at
  `forgejo-repos/processor-*/` is an independent Forgejo-pushable processor
  repository. It owns one internal `processor-*` service, its workflow, code,
  app Compose fragment, and deployment script. The independent
  `forgejo-repos/gateway-c37-118-onboarding/` seed owns the explicit C37.118
  Masterdata publication and catalog-derived legacy-v2 adapter test only. All
  other assets remain in this repository. Every managed seed connects to
  infrastructure only through the external `wama-infra` Docker network.
- Infrastructure monitoring services own `services/victoria-metrics/`,
  `services/node-exporter/`, `services/cadvisor/`, `services/kafka-exporter/`,
  and `services/grafana/`. VictoriaMetrics holds infrastructure telemetry only;
  Common Format records remain out of VictoriaMetrics. Druid owns the live
  measurement store; Grafana owns the pinned Druid datasource plugin and the
  `WAMA Measurements` PMU dashboard. Keep measurement queries on Druid and
  infrastructure telemetry on VictoriaMetrics.

## Stack (PoC mapping vs. production)
| Layer | Production (target) | This PoC |
|-------|--------------------|----------|
| Transport | Kafka via Strimzi operator | Kafka KRaft, plain container |
| Config/CD | Git + Forgejo + ArgoCD (GitOps) | Root repository for infrastructure; Forgejo Actions + app-local `docker compose up -d` only for internal processors and explicit gateway-deployment tests |
| Stream proc | Quixstreams + Measurement Session Exporter | Quixstreams (Python) plus a root-owned Druid-to-Parquet MeasurementSession worker |
| Time-series | Druid | persistent single-server direct raw-Protobuf Kafka ingest |
| Config metadata | PostgreSQL via Kafka Connect | Root-owned Blobmeta materializer now; Kafka Connect later for Masterdata/Schema |
| Infrastructure metrics | Grafana + metrics backend | VictoriaMetrics + node-exporter + cAdvisor + Grafana |
| Raw/blob | SeaweedFS | SeaweedFS `weed mini`, S3-compatible single-node service |
| Viz | Grafana | VictoriaMetrics for infrastructure; Druid-backed PMU measurement dashboard |
| Export | IEC 104 + file export | root-owned one-way IEC 104 exporter; file export remains future work |

## Coding conventions
- Python: type hints, early returns, docstrings, no bare excepts.
- Kafka topics: `LiveMeasurement`, `MeasurementSession`, `Alarm`, `Export`
  (see data contracts).
- Keep raw/waveform data OFF Kafka (separate path in the real design).

## Where the full plan lives
Detailed platform context is in [`docs/wama/`](../docs/wama/). Read:
- [Overview & processes](../docs/wama/00-overview.md)
- [Architecture & SW stack](../docs/wama/01-architecture.md)
- [Data flow & Kafka contracts](../docs/wama/02-dataflow-contracts.md)
- [PoC compose plan](../docs/wama/03-poc-compose-plan.md)
