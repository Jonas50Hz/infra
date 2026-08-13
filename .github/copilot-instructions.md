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

## Stack (PoC mapping vs. production)
| Layer | Production (target) | This PoC |
|-------|--------------------|----------|
| Transport | Kafka via Strimzi operator | Kafka KRaft, plain container |
| Config/CD | Git + Forgejo + ArgoCD (GitOps) | Forgejo Actions + `docker compose up -d` |
| Stream proc | Quixstreams (Python) | Quixstreams (Python) — unchanged |
| Time-series | Druid | skip early; add if needed |
| Raw/blob | SeaweedFS | skip early |
| Viz | Grafana | optional |
| Export | IEC 104 + file export | out of PoC scope |

## Coding conventions
- Python: type hints, early returns, docstrings, no bare excepts.
- Kafka topics: `LiveMeasurement`, `Event`, `Alarm`, `Export` (see data contracts).
- Keep raw/waveform data OFF Kafka (separate path in the real design).

## Where the full plan lives
Detailed platform context is in [`docs/wama/`](../docs/wama/). Read:
- [Overview & processes](../docs/wama/00-overview.md)
- [Architecture & SW stack](../docs/wama/01-architecture.md)
- [Data flow & Kafka contracts](../docs/wama/02-dataflow-contracts.md)
- [PoC compose plan](../docs/wama/03-poc-compose-plan.md)
