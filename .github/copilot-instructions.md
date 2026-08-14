# WAMA PoC — Agent Instructions (always-on)

## What this repo is
Local **docker-compose** PoC of the WAMA data platform. Goal: test Quixstreams
processors against a Kafka backbone, then wire a Forgejo-based CI/CD loop.
This is NOT the production target (production is Kubernetes-based).

## Domain Vocabulary
- [docs/wama/image.png](../docs/wama/image.png),
  [docs/wama/User_Konfiguration_Nexus.bpmn](../docs/wama/User_Konfiguration_Nexus.bpmn),
  and [docs/wama/Livedaten_Prozess_WAMA.bpmn](../docs/wama/Livedaten_Prozess_WAMA.bpmn)
  are authoritative for target WAMA component and process vocabulary.
- `MeasurementSession` is the bounded lifecycle record for start, end, and
  measurement context. It replaces the overly broad `Event` contract; use the
  `MeasurementSession` Kafka topic and `wama-measurement-sessions` bucket.
- The diagram references do not alter the PoC data plane: retain Common Format
  `MCCSMeasurementValue`, raw Protobuf, and the plain Kafka KRaft topology.

## Hard rules (do not violate)
- **No Kubernetes artifacts.** Never generate Helm charts, K8s manifests,
  Strimzi CRDs, or ArgoCD `Application` resources. Those are the production
  stack, not this PoC.
- **Kafka = plain Apache Kafka in KRaft mode** (single broker, no ZooKeeper).
  Non-Confluent stack. No Confluent Schema Registry — use JSON or raw Protobuf;
  add Apicurio only if a registry is explicitly needed.
- **CD replacement:** ArgoCD is K8s-only. For this PoC deploy via a Forgejo
  Actions job running `docker compose up -d`, or Watchtower on new image tags.
- The root `docker-compose.yml` runs infrastructure services only. Application
  processors belong to separately initialized Forgejo repositories under
  `forgejo-repos/`, never to this root Compose assembly.
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
- `infra-readiness` owns `services/infra-readiness/` and is the root stack's
  one-shot behavioral readiness gate. It must remain infrastructure-only and
  must not deploy or validate application processors.
- PostgreSQL owns `services/postgres/` and provides a persistent, initially
  empty target for a future Kafka Connect mirror of compacted `Masterdata`,
  `Schema`, and `Blobmeta` records. Kafka remains the source of truth.
- SeaweedFS owns `services/seaweedfs/` and provides the PoC's authenticated
  S3-compatible raw/waveform and long-term measurement-session storage.
- Forgejo services own `services/forgejo/`, `services/forgejo-init/`, and
  `services/forgejo-runner/`.
- This repository is never pushed to Forgejo. The seed at
  `forgejo-repos/wama-applications/` is the only Forgejo-pushable surface; it
  owns application workflows, processor code, app Compose fragments, and app
  deployment scripts. It connects to infra only through the external
  `wama-infra` Docker network.
- Infrastructure monitoring services own `services/victoria-metrics/`,
  `services/node-exporter/`, `services/cadvisor/`, `services/kafka-exporter/`,
  and `services/grafana/`. VictoriaMetrics holds infrastructure telemetry only;
  Common Format records remain on Kafka and measurement dashboards remain a
  future Druid concern.

## Stack (PoC mapping vs. production)
| Layer | Production (target) | This PoC |
|-------|--------------------|----------|
| Transport | Kafka via Strimzi operator | Kafka KRaft, plain container |
| Config/CD | Git + Forgejo + ArgoCD (GitOps) | Forgejo Actions + `docker compose up -d` |
| Stream proc | Quixstreams + Measurement Session Exporter | Quixstreams (Python); session exporter later |
| Time-series | Druid | skip early; add if needed |
| Config metadata | PostgreSQL via Kafka Connect | PostgreSQL prepared; Kafka Connect later |
| Infrastructure metrics | Grafana + metrics backend | VictoriaMetrics + node-exporter + cAdvisor + Grafana |
| Raw/blob | SeaweedFS | SeaweedFS `weed mini`, S3-compatible single-node service |
| Viz | Grafana | VictoriaMetrics for infrastructure; Druid later for measurement dashboards |
| Export | IEC 104 + file export | out of PoC scope |

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
