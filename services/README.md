# Compose service directories

Each directory in this folder maps one-to-one to a service declared in the root
[`docker-compose.yml`](../docker-compose.yml).

Keep service-owned Dockerfiles, environment files, mounted configuration,
operational scripts, service definition (`compose.yaml`), and service
documentation in the matching directory. The root Compose file owns only
fragment assembly and shared top-level resources.

| Directory | Compose service |
| --- | --- |
| [`kafka/`](kafka/) | `kafka` |
| [`kafka-data-init/`](kafka-data-init/) | `kafka-data-init` |
| [`kafka-init/`](kafka-init/) | `kafka-init` |
| [`kafka-ui/`](kafka-ui/) | `kafka-ui` |
