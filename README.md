# WAMA Data Platform PoC

Local Docker Compose backbone for the WAMA proof of concept. It runs one plain
Apache Kafka broker in KRaft combined mode, initializes the WAMA topic
contract, provides Kafka UI to inspect brokers, topics, consumer groups, and
messages, runs a configurable fake PMU gateway, provides Forgejo with one
Actions runner, and includes SeaweedFS as authenticated S3-compatible blob
storage. Grafana dashboards backed by VictoriaMetrics provide host and Docker
container infrastructure telemetry plus Kafka broker and topic operations
metrics.

This is a Compose-based PoC foundation. It deliberately does not include
ZooKeeper, Confluent components, a Schema Registry, or Kubernetes artifacts.
It does not start a Quixstreams processor by default; use the tracked template
to provision an editable `processor-*` service when one is needed.

## Repository layout

The root [docker-compose.yml](docker-compose.yml) uses Compose `include` to
assemble service fragments and defines only genuinely shared resources. Each
service owns its fragment, configuration, and scripts beneath
[services/](services/):

| Directory | Compose service | Owned assets |
| --- | --- | --- |
| [services/kafka/](services/kafka/) | `kafka` | Compose fragment and broker environment configuration |
| [services/kafka-data-init/](services/kafka-data-init/) | `kafka-data-init` | Compose fragment and Kafka-volume ownership initializer |
| [services/kafka-init/](services/kafka-init/) | `kafka-init` | Compose fragment, topic bootstrap configuration, and script |
| [services/kafka-ui/](services/kafka-ui/) | `kafka-ui` | Compose fragment and Kafka UI environment configuration |
| [services/kafka-exporter/](services/kafka-exporter/) | `kafka-exporter` | Compose fragment for internal Kafka broker, topic, and consumer-lag metrics |
| [services/pmu-gateway/](services/pmu-gateway/) | `pmu-gateway` | Compose fragment, configurable PMU fixture, image, source, and tests |
| [services/seaweedfs/](services/seaweedfs/) | `seaweedfs` | Compose fragment and SeaweedFS S3/admin configuration |
| [services/forgejo/](services/forgejo/) | `forgejo` | Compose fragment and Forgejo server configuration |
| [services/forgejo-init/](services/forgejo-init/) | `forgejo-init` | Compose fragment and empty-instance bootstrap script |
| [services/forgejo-runner/](services/forgejo-runner/) | `forgejo-runner` | Compose fragment, trusted CI image, and host-mode deployment support |
| [services/victoria-metrics/](services/victoria-metrics/) | `victoria-metrics` | Compose fragment, persistent metrics storage, and static scrape configuration |
| [services/node-exporter/](services/node-exporter/) | `node-exporter` | Compose fragment for internal Linux host metrics collection |
| [services/cadvisor/](services/cadvisor/) | `cadvisor` | Compose fragment for internal Docker-container metrics collection |
| [services/grafana/](services/grafana/) | `grafana` | Compose fragment, local credentials, datasource provisioning, and dashboards |

### Provisioned processors
<!-- provisioned-processor-services:start -->
<!-- provisioned-processor-services:end -->

The inactive [processor template](templates/quixstreams-processor/) and the
[provisioning scripts](scripts/) are repository assets rather than Compose
services. They become a service only after provisioning.

Add a matching directory whenever a Compose service is added. Keep the root
Compose file limited to includes and shared resources; place each service's
definition, Dockerfiles, configuration, scripts, and service documentation in
that service directory.

## Prerequisites

- Docker Engine with Docker Compose v2 or newer
- Host ports `127.0.0.1:29092`, `127.0.0.1:8428`, `8080`, `8333`, `23646`,
  `3000`, `3001`, and `2222` available

## Start and stop

Before the first Forgejo start, create a local environment file and replace the
placeholder administrator password. The production-like Actions path requires
an HTTPS hostname that resolves from the Docker host and Actions job containers.

```sh
[ -e .env ] || install -m 600 .env.example .env
```

Before the first Grafana start, initialize its ignored local credential file:

```sh
[ -e services/grafana/grafana.env ] || \
  install -m 600 services/grafana/grafana.env.example \
    services/grafana/grafana.env
```

Grafana's initial administrator username and password are both `wama-admin`.

Start the broker, initialize the topics, start Kafka UI, the fake PMU gateway,
SeaweedFS, and infrastructure monitoring, then provision Forgejo and its single
Actions runner:

```sh
docker compose up -d
```

Set `FORGEJO_DOMAIN`, `FORGEJO_ROOT_URL`, and `FORGEJO_RUNNER_URL` in `.env` to
the HTTPS hostname before enabling image publication and deployment. The local
runner fallback reaches Forgejo through `host.docker.internal`, which is useful
for Actions smoke tests but does not make Docker's registry client trust an HTTP
registry.

Check the service state:

```sh
docker compose ps
docker compose logs kafka-init
docker compose logs forgejo-init
```

`kafka`, `seaweedfs`, `victoria-metrics`, and `grafana` should be `healthy`,
`kafka-init` should have exited with status `0`, and `kafka-ui`,
`kafka-exporter`, `pmu-gateway`, `node-exporter`, `cadvisor`, `forgejo`, and
`forgejo-runner` should be running. `forgejo-init` should have exited with
status `0` after creating the initial account and runner registration.

Stop the stack while preserving Kafka, SeaweedFS, Forgejo, runner, Grafana,
and VictoriaMetrics data:

```sh
docker compose down
```

Start it again with `docker compose up -d`. Kafka topic initialization and
Forgejo runner registration are idempotent.

To remove all local state and begin with an empty broker, object store, Forgejo
instance, dashboards, and metrics history, stop the stack and remove its
Compose-managed volumes:

```sh
docker compose down -v
```

This removes the Forgejo administrator, all repositories, and the runner
registration as well as Kafka data, every SeaweedFS object, Grafana state, and
VictoriaMetrics history.

## Forgejo Actions

`forgejo-init` creates the configured administrator, a private
`<owner>/<repository>` repository, the deployment-root marker, and one
repository-scoped runner named `wama-ci`. It stores generated runner credentials
in `forgejo-runner-data`; administrator credentials and the deployment `.env`
remain outside Git. The initial repository is empty, so push this checkout to
the generated remote after starting the stack.

```sh
git remote add forgejo "$FORGEJO_ROOT_URL$FORGEJO_BOOTSTRAP_ADMIN_USERNAME/$FORGEJO_BOOTSTRAP_REPOSITORY.git"
git push --set-upstream forgejo main
```

[`.forgejo/workflows/processors.yaml`](.forgejo/workflows/processors.yaml)
validates pull requests, then on `main` tests application images, publishes
`sha-<commit>` and `main` OCI tags to Forgejo Packages, and serializes a Compose
deployment. Images are named `<forgejo-host>/<owner>/<repository>/<service>`.

The `wama-ci` label runs containerized jobs. `wama-deploy` runs the deploy job
inside the runner container with the host Docker socket and the absolute
`WAMA_DEPLOY_ROOT` mounted at the same path. This is deliberately trusted local
PoC access: anyone able to run an arbitrary workflow in the scoped repository
can control the Docker host. Do not expose this runner to untrusted users,
repositories, forks, or production workloads.

To create an editable processor service, run:

```sh
python3 scripts/provision_processor.py frequency-scale
```

Edit the generated `pipeline.py` and optional YAML configuration, then validate
the service contract locally:

```sh
python3 scripts/validate_services.py
```

## Fake PMU messages

`pmu-gateway` loads
[services/pmu-gateway/messages.yaml](services/pmu-gateway/messages.yaml) once
at startup and continuously publishes its configured scalar PMU measurements in
fixture order. The default fixture contains three-phase voltage and current,
frequency, and ROCOF records.

Each entry needs an `mrid` and exactly one typed Common Format value. Supported
value names are `double_value`, `int_value`, `uint_value`, `bool_value`,
`string_value`, and `timestamp_value`; see the service
[configuration reference](services/pmu-gateway/README.md). The gateway creates
Kafka, gateway, and MCCS timestamps at send time. It can optionally derive a
field timestamp from `field_timestamp_offset_ms`.

Change the default fixture, then recreate the gateway to apply it:

```sh
docker compose up -d --force-recreate pmu-gateway
```

To select a different fixture for startup, use an absolute host path:

```sh
PMU_GATEWAY_CONFIG_SOURCE="$PWD/my-pmu-messages.yaml" \
  PMU_GATEWAY_PUBLISH_INTERVAL_MS=250 \
  docker compose up -d --force-recreate pmu-gateway
```

## Access

| Purpose | Address |
| --- | --- |
| Kafka UI | `http://<host-ip>:8080` (including <http://127.0.0.1:8080>) |
| Kafka from the host | `localhost:29092` |
| Kafka from another Compose service | `kafka:9092` |
| SeaweedFS S3 API | `http://<host-ip>:8333` (including <http://127.0.0.1:8333>) |
| SeaweedFS S3 from another Compose service | `http://seaweedfs:8333` |
| SeaweedFS admin UI | `http://<host-ip>:23646` |
| Forgejo | `https://<forgejo-domain>` when configured for CI/CD |
| Forgejo Git over SSH | `ssh://git@<host-ip>:2222/<owner>/<repository>.git` |
| Grafana | `http://<host-ip>:3001` |
| VictoriaMetrics API and VMUI | `http://127.0.0.1:8428` |

Kafka UI exposes topic contents, partitions, consumer groups, messages, and
broker metadata. Its port is exposed on all host interfaces so it can be opened
from the LAN. Forgejo HTTP and SSH are likewise exposed on all host interfaces.
Grafana is LAN-accessible on port 3001; VictoriaMetrics is available only from
the Docker host, and node-exporter, cAdvisor, and kafka-exporter expose no host
ports.

## Infrastructure monitoring

VictoriaMetrics directly scrapes itself, Grafana, node-exporter, cAdvisor, and
kafka-exporter every 15 seconds and retains the resulting infrastructure metrics
for one month in its Compose-managed volume. Kafka exporter emits operational
broker, topic, partition, offset, and consumer-lag metadata only; it does not
copy Kafka messages, Common Format records, raw data, waveforms, events, or
alarms into VictoriaMetrics.

Grafana provisions the `VictoriaMetrics` datasource and the following
read-only dashboards under the **WAMA Infrastructure** folder:

- **WAMA Infrastructure Overview**: scrape health, host CPU, memory, load,
  filesystem, network, and monitoring-process memory.
- **WAMA Compose Containers**: per-Compose-service container presence, CPU,
  memory, and network traffic.
- **WAMA Kafka Operations**: exporter health, broker and topic counts,
  replication state, per-topic produce rate and offsets, and consumer-group lag
  when consumers exist.

No alert rules, contact points, or notification delivery are provisioned in
this PoC slice. Set `GRAFANA_ROOT_URL=http://<host-ip>:3001/` when Grafana must
generate external URLs for a LAN address. Grafana applies the administrator
password only while first initializing its `grafana-data` volume; rotate an
existing password through Grafana rather than replacing the local environment
file.

## S3-compatible storage

`seaweedfs` runs a single-node SeaweedFS `weed mini` instance. Its S3 gateway
requires signed requests from startup and the admin UI requires a login. Both
`wama-raw` and `wama-events` are created idempotently when the service starts;
their objects persist in the `seaweedfs-data` Compose volume.

| Bucket | Purpose |
| --- | --- |
| `wama-raw` | Raw samples and waveform objects kept off Kafka |
| `wama-events` | Long-term event objects kept off Kafka |

| Interface | Access key or user | Secret or password |
| --- | --- | --- |
| S3 | `wama-s3-admin` | `wama-s3-admin-secret` |
| Admin UI | `wama-admin` | `wama-admin` |

These are intentionally public credentials for this trusted local PoC. They
must not be reused outside it. No S3 lifecycle policy is configured: raw-data
retention remains an open `X weeks` decision, while event objects are intended
for long-term retention.

List the initialized buckets from the service container:

```sh
printf 's3.bucket.list\n' | docker compose exec -T seaweedfs weed shell
```

With the AWS CLI installed, list the buckets from the host:

```sh
AWS_ACCESS_KEY_ID=wama-s3-admin \
AWS_SECRET_ACCESS_KEY=wama-s3-admin-secret \
AWS_DEFAULT_REGION=us-east-1 \
aws --endpoint-url http://127.0.0.1:8333 s3 ls
```

## Initialized topic contract

| Topic | Type | Cleanup policy | Purpose |
| --- | --- | --- | --- |
| `LiveMeasurement` | stream | `delete` | `MCCSMeasurementValue` Common Format measurements and derived values |
| `Event` | stream | `delete` | Detected and classified events |
| `Alarm` | stream | `delete` | Alarm records |
| `Export` | stream | `delete` | Records for real-time and file export |
| `Masterdata` | compacted | `compact` | Source masterdata and capabilities |
| `Schema` | compacted | `compact` | Common Format schema definitions |
| `Blobmeta` | compacted | `compact` | SeaweedFS blob pointers and metadata |

All topics have one partition and replication factor one because this is a
single-broker PoC. Automatic topic creation is disabled. No retention period
is set beyond Kafka's broker default because the data-retention decision remains
open.

`LiveMeasurement` uses raw Protobuf. `pmu-gateway` serializes
`MCCSMeasurementValue` records directly from
[the canonical schema](docs/wama/schema/rtd_schema.proto); see
[the Common Format contract](docs/wama/02-dataflow-contracts.md). Do not use
JSON console messages as application data on that topic.

## CLI inspection

List the WAMA topics:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --list \
  --exclude-internal
```

Describe a topic:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --topic LiveMeasurement
```

Check its cleanup policy:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka:9092 \
  --describe \
  --entity-type topics \
  --entity-name Masterdata
```

## Test Kafka UI with a disposable message

Create a temporary topic, publish a text message, then open Kafka UI and select
`kafka-validation` to inspect the message:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --create \
  --topic kafka-validation \
  --partitions 1 \
  --replication-factor 1

printf 'Kafka UI validation\n' | \
  docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka:9092 \
    --topic kafka-validation
```

Confirm it from the command line:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic kafka-validation \
  --from-beginning \
  --max-messages 1
```

Delete the temporary topic after validation:

```sh
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --delete \
  --topic kafka-validation
```

## PoC network and security model

The Kafka broker remains bound to `127.0.0.1:29092`; Compose services use the
internal `kafka:9092` endpoint. Kafka UI is intentionally exposed on
`0.0.0.0:8080` and has unrestricted access to the local broker without
authentication. Forgejo is exposed over plaintext HTTP and SSH with its
documented disposable administrator credentials. SeaweedFS exposes its S3 API
and admin UI over plaintext HTTP on all host interfaces; its documented PoC
credentials have broad local access.

Grafana is exposed on port 3001 with anonymous access and self-registration
disabled. Its administrator credentials live only in the ignored
`services/grafana/grafana.env` file. VictoriaMetrics is bound to localhost;
node-exporter and cAdvisor are Compose-internal. cAdvisor runs privileged and
mounts Docker and host paths to collect container metrics, so it carries the
same trusted-host restriction as the Actions runner.

The sole Actions runner mounts the host Docker socket and uses Docker socket
automount for workflow job containers. A workflow can therefore control the
host Docker daemon and every container it manages. This is intentional for the
PoC's later build and `docker compose up -d` deployment loop. Use this stack
only on a trusted LAN. Do not use it as a production deployment without adding
appropriate authentication, encryption, authorization, backup, high
availability, runner isolation, and multi-broker durability.
