# Compose service directories

Each directory in this folder maps one-to-one to a service declared in the root
[`docker-compose.yml`](../docker-compose.yml).

Keep service-owned Dockerfiles, environment files, mounted configuration,
operational scripts, service definition (`compose.yaml`), and service
documentation in the matching directory. The root Compose file owns only
fragment assembly and shared top-level resources.

[`../templates/quixstreams-processor/`](../templates/quixstreams-processor/)
is intentionally not a Compose service. Use `python3
scripts/provision_processor.py <name>` from the repository root to create a
tracked `processor-<name>` service and register it below.

| Directory | Compose service |
| --- | --- |
| [`kafka/`](kafka/) | `kafka` |
| [`kafka-data-init/`](kafka-data-init/) | `kafka-data-init` |
| [`kafka-init/`](kafka-init/) | `kafka-init` |
| [`kafka-ui/`](kafka-ui/) | `kafka-ui` |
| [`kafka-exporter/`](kafka-exporter/) | `kafka-exporter` |
| [`pmu-gateway/`](pmu-gateway/) | `pmu-gateway` |
| [`seaweedfs/`](seaweedfs/) | `seaweedfs` |
| [`forgejo/`](forgejo/) | `forgejo` |
| [`forgejo-init/`](forgejo-init/) | `forgejo-init` |
| [`forgejo-runner/`](forgejo-runner/) | `forgejo-runner` |
| [`victoria-metrics/`](victoria-metrics/) | `victoria-metrics` |
| [`node-exporter/`](node-exporter/) | `node-exporter` |
| [`cadvisor/`](cadvisor/) | `cadvisor` |
| [`grafana/`](grafana/) | `grafana` |

## Provisioned processors
<!-- provisioned-processor-services:start -->
<!-- provisioned-processor-services:end -->
