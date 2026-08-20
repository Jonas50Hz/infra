# Compose service directories

Each directory in this folder maps one-to-one to a service declared in the root
[`docker-compose.yml`](../docker-compose.yml).

Keep service-owned Dockerfiles, environment files, mounted configuration,
operational scripts, service definition (`compose.yaml`), and service
documentation in the matching directory. The root Compose file owns only
fragment assembly and shared top-level resources.

This directory is infrastructure-only. Do not add application `processor-*`
services here. The separate
[`../forgejo-repos/`](../forgejo-repos/) seeds each own one processor service
and join this stack through the external `wama-infra` network.

| Directory | Compose service |
| --- | --- |
| [`kafka/`](kafka/) | `kafka` |
| [`kafka-data-init/`](kafka-data-init/) | `kafka-data-init` |
| [`kafka-init/`](kafka-init/) | `kafka-init` |
| [`kafka-exporter/`](kafka-exporter/) | `kafka-exporter` |
| [`kafka-ui/`](kafka-ui/) | `kafka-ui` |
| [`pmu-gateway/`](pmu-gateway/) | `pmu-gateway` |
| [`iec104-exporter/`](iec104-exporter/) | `iec104-exporter` |
| [`iec104-receiver/`](iec104-receiver/) | `iec104-receiver` (profile `iec104-test`) |
| [`iec104-browser/`](iec104-browser/) | `iec104-browser` |
| [`druid/`](druid/) | `druid` |
| [`druid-init/`](druid-init/) | `druid-init` |
| [`postgres/`](postgres/) | `postgres` |
| [`seaweedfs/`](seaweedfs/) | `seaweedfs` |
| [`measurement-session-exporter/`](measurement-session-exporter/) | `measurement-session-exporter` |
| [`measurement-session-api/`](measurement-session-api/) | `measurement-session-api` |
| [`measurement-session-browser/`](measurement-session-browser/) | `measurement-session-browser` |
| [`measurement-session-e2e/`](measurement-session-e2e/) | `measurement-session-e2e` |
| [`forgejo/`](forgejo/) | `forgejo` |
| [`forgejo-init/`](forgejo-init/) | `forgejo-init` |
| [`forgejo-runner/`](forgejo-runner/) | `forgejo-runner` |
| [`victoria-metrics/`](victoria-metrics/) | `victoria-metrics` |
| [`node-exporter/`](node-exporter/) | `node-exporter` |
| [`cadvisor/`](cadvisor/) | `cadvisor` |
| [`grafana/`](grafana/) | `grafana` (VictoriaMetrics infrastructure and Druid PMU dashboards) |
| [`infra-readiness/`](infra-readiness/) | `infra-readiness` |
