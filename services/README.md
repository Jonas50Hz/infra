# Compose service directories

Each directory in this folder maps one-to-one to a service declared in the root
[`docker-compose.yml`](../docker-compose.yml).

Keep service-owned Dockerfiles, environment files, mounted configuration,
operational scripts, service definition (`compose.yaml`), and service
documentation in the matching directory. The root Compose file owns only
fragment assembly and shared top-level resources.

This directory is infrastructure-only. Do not add application `processor-*`
services here. The separate
[`../forgejo-repos/wama-applications/`](../forgejo-repos/wama-applications/)
seed owns processor services and joins this stack through the external
`wama-infra` network.

| Directory | Compose service |
| --- | --- |
| [`kafka/`](kafka/) | `kafka` |
| [`kafka-data-init/`](kafka-data-init/) | `kafka-data-init` |
| [`kafka-init/`](kafka-init/) | `kafka-init` |
| [`kafka-exporter/`](kafka-exporter/) | `kafka-exporter` |
| [`kafka-ui/`](kafka-ui/) | `kafka-ui` |
| [`pmu-gateway/`](pmu-gateway/) | `pmu-gateway` |
| [`postgres/`](postgres/) | `postgres` |
| [`seaweedfs/`](seaweedfs/) | `seaweedfs` |
| [`forgejo/`](forgejo/) | `forgejo` |
| [`forgejo-init/`](forgejo-init/) | `forgejo-init` |
| [`forgejo-runner/`](forgejo-runner/) | `forgejo-runner` |
| [`victoria-metrics/`](victoria-metrics/) | `victoria-metrics` |
| [`node-exporter/`](node-exporter/) | `node-exporter` |
| [`cadvisor/`](cadvisor/) | `cadvisor` |
| [`grafana/`](grafana/) | `grafana` |
| [`infra-readiness/`](infra-readiness/) | `infra-readiness` |
