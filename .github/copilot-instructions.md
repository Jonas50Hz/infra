# WAMA PoC — Agent Instructions (always-on)

## What this repo is
Local **docker-compose** PoC of the WAMA data platform. Goal: test Quixstreams
processors against a Kafka backbone, then wire a Forgejo-based CI/CD loop.
This is NOT the production target (production is Kubernetes-based).

## Hard rules (do not violate)
- **No Kubernetes artifacts.** Never generate Helm charts, K8s manifests,
  Strimzi CRDs, or ArgoCD `Application` resources. Those are the production
  stack, not this PoC.
- **Kafka = plain Apache Kafka in KRaft mode** (single broker, no ZooKeeper).
  Non-Confluent stack. No Confluent Schema Registry — use JSON or raw Protobuf;
  add Apicurio only if a registry is explicitly needed.
- **CD replacement:** ArgoCD is K8s-only. For this PoC deploy via a Forgejo
  Actions job running `docker compose up -d`, or Watchtower on new image tags.
- Everything runs as services in `docker-compose.yml`. One service per processor.
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
- Processor services are created from `templates/quixstreams-processor/` with
  `scripts/provision_processor.py`; each owns editable Python/Quixstreams code,
  its test suite, and optional YAML configuration.
- SeaweedFS owns `services/seaweedfs/` and provides the PoC's authenticated
  S3-compatible raw/waveform and long-term event storage.
- Forgejo services own `services/forgejo/`, `services/forgejo-init/`, and
  `services/forgejo-runner/`.
- The Forgejo Actions workflow lives in `.forgejo/workflows/`. Reuse the
  existing `forgejo-init` bootstrap and `forgejo-runner` services; do not add
  a parallel runner/bootstrap service. `wama-deploy` has host Docker access
  only for this trusted Compose PoC and must stay repository-scoped.
- Infrastructure monitoring services own `services/victoria-metrics/`,
  `services/node-exporter/`, `services/cadvisor/`, `services/kafka-exporter/`,
  and `services/grafana/`. VictoriaMetrics holds infrastructure telemetry only;
  Kafka exporter contributes broker/topic metadata, while Common Format records
  remain on Kafka and measurement dashboards remain a future Druid concern.

### Provisioned processors
<!-- provisioned-processors:start -->
<!-- provisioned-processors:end -->

## Stack (PoC mapping vs. production)
| Layer | Production (target) | This PoC |
|-------|--------------------|----------|
| Transport | Kafka via Strimzi operator | Kafka KRaft, plain container |
| Config/CD | Git + Forgejo + ArgoCD (GitOps) | Forgejo Actions + `docker compose up -d` |
| Stream proc | Quixstreams (Python) | Quixstreams (Python) — unchanged |
| Time-series | Druid | skip early; add if needed |
| Infrastructure metrics | Grafana + metrics backend | VictoriaMetrics + node-exporter + cAdvisor + kafka-exporter + Grafana |
| Raw/blob | SeaweedFS | SeaweedFS `weed mini`, S3-compatible single-node service |
| Viz | Grafana | VictoriaMetrics for infrastructure; Druid later for measurement dashboards |
| Export | IEC 104 + file export | out of PoC scope |

## Coding conventions
- Python: type hints, early returns, docstrings, no bare excepts.
- Kafka topics: `LiveMeasurement`, `Event`, `Alarm`, `Export` (see data contracts).
- Keep raw/waveform data OFF Kafka (separate path in the real design).
- For local PoC services that support username/password authentication, use
  `wama-admin` for both values where the service permits it. Preserve
  service-native access-key/secret and token credentials instead.

## Where the full plan lives
Detailed platform context is in [`docs/wama/`](../docs/wama/). Read:
- [Overview & processes](../docs/wama/00-overview.md)
- [Architecture & SW stack](../docs/wama/01-architecture.md)
- [Data flow & Kafka contracts](../docs/wama/02-dataflow-contracts.md)
- [PoC compose plan](../docs/wama/03-poc-compose-plan.md)
