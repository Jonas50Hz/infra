# WAMA Data Platform PoC

Local Docker Compose backbone for the WAMA proof of concept. It runs one plain
Apache Kafka broker in KRaft combined mode, initializes the WAMA topic
contract, provides Kafka UI to inspect brokers, topics, consumer groups, and
messages, runs a configurable fake PMU gateway, provides Forgejo with one
Actions runner, includes SeaweedFS as authenticated S3-compatible blob storage,
and materializes finalized measurement sessions into an immutable PostgreSQL
catalog. A static browser exposes those sessions anonymously through an API
that proxies integrity-checked artifacts from SeaweedFS. Grafana dashboards
backed by VictoriaMetrics provide host and Docker container infrastructure
telemetry plus Kafka broker and topic operations metrics, while a Druid-backed
dashboard displays live PMU measurements over time. A persistent single-server
Druid instance directly ingests raw-Protobuf `LiveMeasurement` records for live
SQL queries.

This is a Compose-based PoC foundation. It deliberately does not include
a Kafka ZooKeeper service, Confluent components, a Schema Registry, a
Quixstreams processor in the root Compose assembly, or Kubernetes artifacts.
Druid's internal single-server coordination is confined to its container and
does not change the plain Kafka KRaft topology.

## Repository Boundary

This checkout is the **infrastructure repository**. It owns the Compose stack,
Kafka backbone, source gateway, Forgejo server and runner, storage, and
monitoring. It is the ownership and deployment surface for all infrastructure,
including the Druid image and supervisor,
and every asset outside the narrow Forgejo deployment scope; in particular, the
current `pmu-gateway` remains an infrastructure service here. This checkout is
never added as a Forgejo remote and is never pushed to Forgejo.

[`forgejo-repos/wama-processors/`](forgejo-repos/wama-processors/) is a
separate processors-repository seed. `forgejo-init` automatically creates a
private Forgejo repository and seeds its `main` branch from this directory when
the remote has no refs. An existing nonempty private repository is left
unchanged. It contains internal processor code, processor-only Compose fragments
and deployment tooling, and a Forgejo Actions workflow. A future gateway may
use that workflow only as an explicit gateway-deployment test; it does not move
the current `pmu-gateway` or any other infrastructure service out of this
checkout. Processor containers connect to this stack through the external
`wama-infra` Docker network; they do not include, modify, or redeploy this
Compose project.

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
| [services/kafka-exporter/](services/kafka-exporter/) | `kafka-exporter` | Compose fragment for internal Kafka broker, topic, and consumer-lag metrics |
| [services/kafka-ui/](services/kafka-ui/) | `kafka-ui` | Compose fragment and Kafka UI environment configuration |
| [services/pmu-gateway/](services/pmu-gateway/) | `pmu-gateway` | Compose fragment, configurable PMU fixture, image, source, and tests |
| [services/druid/](services/druid/) | `druid` | Persistent single-server Druid image, canonical Protobuf descriptor build, and Router API |
| [services/druid-init/](services/druid-init/) | `druid-init` | Idempotent `LiveMeasurement` Kafka supervisor initializer and tests |
| [services/postgres/](services/postgres/) | `postgres` | Compose fragment, trusted local credentials, and persistent prepared database |
| [services/seaweedfs/](services/seaweedfs/) | `seaweedfs` | Compose fragment and SeaweedFS S3/admin configuration |
| [services/measurement-session-exporter/](services/measurement-session-exporter/) | `measurement-session-exporter` | Profile-gated one-shot finalized-session fixture exporter |
| [services/measurement-session-api/](services/measurement-session-api/) | `measurement-session-api` | Raw-Protobuf catalog materializer and anonymous read-only artifact proxy |
| [services/measurement-session-browser/](services/measurement-session-browser/) | `measurement-session-browser` | Disposable static browser and same-origin API proxy |
| [services/measurement-session-e2e/](services/measurement-session-e2e/) | `measurement-session-e2e` | Profile-gated contract-to-download verification service |
| [services/forgejo/](services/forgejo/) | `forgejo` | Compose fragment and Forgejo server configuration |
| [services/forgejo-init/](services/forgejo-init/) | `forgejo-init` | Compose fragment and empty-instance bootstrap script |
| [services/forgejo-runner/](services/forgejo-runner/) | `forgejo-runner` | Compose fragment for the single Forgejo Actions runner |
| [services/victoria-metrics/](services/victoria-metrics/) | `victoria-metrics` | Compose fragment, persistent metrics storage, and static scrape configuration |
| [services/node-exporter/](services/node-exporter/) | `node-exporter` | Compose fragment for internal Linux host metrics collection |
| [services/cadvisor/](services/cadvisor/) | `cadvisor` | Compose fragment for internal Docker-container metrics collection |
| [services/grafana/](services/grafana/) | `grafana` | Pinned Druid plugin image, local credentials, datasource provisioning, and dashboards |
| [services/infra-readiness/](services/infra-readiness/) | `infra-readiness` | One-shot behavioral readiness gate for the complete infrastructure stack |

Add a matching directory whenever a Compose service is added. Keep the root
Compose file limited to includes and shared resources; place each service's
definition, Dockerfiles, configuration, scripts, and service documentation in
that service directory.

`processor-*` services do not belong under this infrastructure `services/`
directory. Provision them in the separate processors repository seed.

## Prerequisites

- Docker Engine with Docker Compose v2 or newer
- Host ports `127.0.0.1:29092`, `127.0.0.1:8428`, `8080`, `8333`, `23646`,
  `3000`, `3001`, `3002`, `5432`, `8888`, and `2222` available

## Start and stop

All inputs required for the trusted local PoC, including public Forgejo and
Grafana credentials, are tracked. A fresh clone starts the complete
infrastructure with one command:

```sh
docker compose up -d
```

Kafka-dependent services wait for the broker to become healthy and topic
initialization to complete before starting, including after `docker compose stop`
followed by `docker compose up -d`.

`infra-readiness` is the final one-shot gate. It retries for up to three
minutes while Grafana provisioning and VictoriaMetrics scraping settle, then
exits with status `0` only after the complete infrastructure is ready for
processors.

To override the local defaults for a public HTTPS Forgejo endpoint or a
nondefault application deployment root, first create `.env` from
[.env.example](.env.example), then set `FORGEJO_DOMAIN`, `FORGEJO_ROOT_URL`,
and `FORGEJO_RUNNER_URL`. That hostname must resolve from the Docker host and
Forgejo Actions job containers. The local `host.docker.internal` runner
endpoint is suitable for bootstrap and Actions smoke checks, but Docker will
not treat an HTTP registry as trusted by default.

```sh
[ -e .env ] || install -m 600 .env.example .env
```

Check the service state:

```sh
docker compose ps --all
docker compose logs kafka-init
docker compose logs forgejo-init
docker compose logs infra-readiness
```

`kafka`, `druid`, `postgres`, `seaweedfs`, `victoria-metrics`, `grafana`, and
`measurement-session-api` should be `healthy`; `kafka-init` and `druid-init`
should have exited with status `0`; and `kafka-ui`, `kafka-exporter`, `pmu-gateway`,
`measurement-session-browser`, `node-exporter`, `cadvisor`, `forgejo`, and
`forgejo-runner` should be running. `forgejo-init` and `infra-readiness` should
have exited with status `0`; the latter verifies the Kafka contract and PMU
traffic, Druid supervisor and SQL query, PostgreSQL, SeaweedFS S3, Forgejo, and
the monitoring path. The profile-gated exporter and E2E verifier run only when
explicitly invoked.

Stop the stack while preserving Kafka, Druid, PostgreSQL, SeaweedFS, Forgejo,
runner, Grafana, and VictoriaMetrics data:

```sh
docker compose down
```

Start it again with `docker compose up -d`. Kafka topic initialization and
Forgejo runner registration and processors repository seeding are idempotent.

To remove all local state and begin with an empty broker, database, object
store, Forgejo instance, dashboards, and metrics history, stop the stack and
remove its Compose-managed volumes:

```sh
docker compose down -v
```

This removes the Forgejo administrator, all repositories, and the runner
registration as well as Kafka data, every PostgreSQL database, every SeaweedFS
object, Druid metadata and segments, Grafana state, and VictoriaMetrics history.

## Lifecycle validation

Run the destructive clean-start and persistent-restart validation before
provisioning processors:

```sh
scripts/test-infrastructure-lifecycle.sh
```

The script first runs the non-destructive Forgejo bootstrap safety checks, then
runs `docker compose down -v --remove-orphans`, starts the ordinary stack,
requires `infra-readiness` to succeed, and repeats the check after `docker
compose down` without deleting volumes. It independently re-runs the Druid PMU
query check after each start. On failure it leaves the stack running and prints
focused diagnostic logs. On success it leaves the restarted stack running for
processor testing.

Run only the Druid/Kafka/PMU path during focused local development:

```sh
scripts/test-druid-ingestion.sh
```

Run the Druid-backed Grafana dashboard path:

```sh
scripts/test-grafana-pmu-dashboard.sh
```

## CI

The root [infrastructure workflow](.github/workflows/infrastructure.yml) renders
the Compose assembly, validates the Druid descriptor/image and helper tests,
proves PMU-to-Druid ingestion and the Grafana PMU dashboard path, runs every
finalized-session image test target, and proves the complete contract-to-download
path on an ephemeral Docker runner. It only validates this root repository; it
does not publish images or deploy services. The separate Forgejo workflow remains
restricted to the processors repository.

## Forgejo Actions

`forgejo-init` creates the configured administrator, a private
`<owner>/wama-processors` repository, its initial `main` commit, and the
`.wama-forgejo-processors-root` marker in the isolated processors deployment
root. It skips seeding without modifying an existing repository that already
has refs, and it fails without mutation if that existing repository is not
private. Its generated runner credentials remain in the `forgejo-runner-data`
volume.

After bootstrap, clone the separate processors repository to work on it. These
commands must never be run from this infrastructure checkout:

```sh
git clone "https://<forgejo-host>/<owner>/wama-processors.git"
cd wama-processors
```

The processors seed's workflow validates pull requests. A trusted processors
`main` push tests and publishes only `processor-*` images, then synchronizes
only the processors checkout to `WAMA_PROCESSORS_DEPLOY_ROOT` and deploys only
those processor services. A gateway may use Forgejo only in a deliberately declared
gateway-deployment test; that exception must not deploy, modify, or take
ownership of the current `pmu-gateway` or any root infrastructure service.

The `wama-processors-ci` and `wama-processors-deploy` labels belong to separate
repository-scoped connections handled by the same capacity-one runner daemon.
The deployment connection runs in the runner container with the host Docker
socket and processors deployment root mounted at the same path. This is trusted
local-PoC access: processors-repository workflow authors can control the Docker
host. Do not expose this runner to untrusted repositories, users, or production
workloads.

Create processors only from the instructions in the separate
[processors repository README](forgejo-repos/wama-processors/README.md).
Each processor owns its Python code, test suite, and app-local Compose fragment.

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
| PostgreSQL from the host | `postgresql://<host-ip>:5432/wama` |
| PostgreSQL from another Compose service | `postgresql://postgres:5432/wama` |
| SeaweedFS S3 API | `http://<host-ip>:8333` (including <http://127.0.0.1:8333>) |
| SeaweedFS S3 from another Compose service | `http://seaweedfs:8333` |
| SeaweedFS admin UI | `http://<host-ip>:23646` |
| Forgejo | `http://<host-ip>:3000` |
| Forgejo Git over SSH | `ssh://git@<host-ip>:2222/<owner>/<repository>.git` |
| Grafana | `http://<host-ip>:3001` |
| PMU live dashboard | `http://<host-ip>:3001/d/wama-pmu-live-measurements/wama-pmu-live-measurements` |
| Measurement session browser | `http://<host-ip>:3002` |
| Druid Router API and web console | `http://<host-ip>:8888` |
| VictoriaMetrics API and VMUI | `http://127.0.0.1:8428` |

Kafka UI exposes topic contents, partitions, consumer groups, messages, and
broker metadata. Its port is exposed on all host interfaces so it can be opened
from the LAN. Forgejo HTTP and SSH are likewise exposed on all host interfaces.
Grafana and PostgreSQL are LAN-accessible on ports 3001 and 5432 respectively;
VictoriaMetrics is available only from the Docker host, and node-exporter plus
cAdvisor expose no host ports. Druid exposes only its Router on port 8888 to all
host interfaces; its Coordinator, Overlord, Broker, Historical, task runner,
and in-container coordination endpoints have no host mappings.

## Infrastructure monitoring

VictoriaMetrics directly scrapes itself, Grafana, node-exporter, and cAdvisor
every 15 seconds and retains the resulting infrastructure metrics for one month
in its Compose-managed volume. It is not a destination for Common Format,
Kafka, raw, waveform, measurement-session, or alarm data.

Grafana provisions the `VictoriaMetrics` datasource and the following
read-only dashboards under the **WAMA Infrastructure** folder:

- **WAMA Infrastructure Overview**: scrape health, host CPU, memory, load,
  filesystem, network, and monitoring-process memory.
- **WAMA Compose Containers**: per-Compose-service container presence, CPU,
  memory, and network traffic.

Grafana also provisions the internal `Druid` datasource through
`http://druid:8888` and the following read-only dashboard under
**WAMA Measurements**:

- **WAMA PMU Live Measurements**: valid PMU voltage, current, frequency, and
  ROCOF values as separate unit-safe trends plus recent timestamp evidence.

No alert rules, contact points, or notification delivery are provisioned in
this PoC slice. Set `GRAFANA_ROOT_URL=http://<host-ip>:3001/` when Grafana must
generate external URLs for a LAN address.

The Druid plugin is pinned in the local Grafana image and is used only for
Common Format measurement queries. VictoriaMetrics remains infrastructure-only;
Common Format records are not copied into it. Alerting and Trino federation
remain deferred.

## Druid live measurements

`druid` is a root-owned, persistent Apache Druid single-server service. Its
image generates `/opt/wama/rtd_schema.desc` from the canonical
[Common Format schema](docs/wama/schema/rtd_schema.proto) at build time and
loads Druid's Kafka and Protobuf extensions. `druid-init` idempotently creates
or updates the `live_measurements` Kafka supervisor after Druid is healthy.

The supervisor reads raw `MCCSMeasurementValue` values directly from
`LiveMeasurement` at `kafka:9092`. It uses the Kafka record timestamp as
`__time`, which the PMU gateway contract already equates with `timestamp_mccs`,
and preserves the typed scalar value alternatives plus quality and source
timestamps. `live_measurements` uses `queryGranularity: none` and `rollup:
false`.

Query the configured PMU frequency record through the LAN-accessible Router:

```sh
curl --request POST "http://<host-ip>:8888/druid/v2/sql" \
  --header 'Content-Type: application/json' \
  --data '{"query":"SELECT \"__time\", \"mrid\", \"double_value\", \"quality_valid\" FROM \"live_measurements\" WHERE \"mrid\" = '\''urn:wama:poc:pmu:bay-01:frequency'\'' ORDER BY \"__time\" DESC LIMIT 1"}'
```

Druid state persists in the Compose-managed `druid-data` volume. No Druid
retention, deletion, compaction, rollup, or aggregation policy is configured in
this PoC. Grafana renders the raw valid-PMU dashboard over this datasource;
alert rules remain unconfigured. Druid image publishing and deployment remain
root-local Compose work; the processor-only Forgejo workflow does not publish or
deploy Druid.

## S3-compatible storage

`seaweedfs` runs a single-node SeaweedFS `weed mini` instance. Its S3 gateway
requires signed requests from startup and the admin UI requires a login. Both
`wama-raw` and `wama-measurement-sessions` are created idempotently when the
service starts; their objects persist in the `seaweedfs-data` Compose volume.

| Bucket | Purpose |
| --- | --- |
| `wama-raw` | Raw samples and waveform objects kept off Kafka |
| `wama-measurement-sessions` | Long-term measurement-session objects kept off Kafka |

| Interface | Access key or user | Secret or password |
| --- | --- | --- |
| S3 | `wama-s3-admin` | `wama-s3-admin-secret` |
| Admin UI | `wama-admin` | `wama-admin` |

These are intentionally public credentials for this trusted local PoC. They
must not be reused outside it. No S3 lifecycle policy is configured: raw-data
retention remains an open `X weeks` decision, while measurement-session objects
are intended for long-term retention.

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

## PostgreSQL

`postgres` initializes an empty `wama` database and role on its first start and
persists them in the `postgres-data` Compose volume. Once
`measurement-session-api` starts, it creates the immutable
`measurement_session_catalog` schema from the `MeasurementSession` Kafka topic.
Kafka remains the source of truth. PostgreSQL is also prepared for a future
Kafka Connect mirror of compacted `Masterdata`, `Schema`, and `Blobmeta`;
that connector is not part of this PoC.

| Setting | Value |
| --- | --- |
| Database | `wama` |
| User | `wama` |
| Password | `wama-postgres-password` |

These are intentionally public credentials for this trusted local PoC. Because
PostgreSQL is LAN-accessible, do not reuse them outside this environment.

Check that the initialized database accepts connections:

```sh
docker compose exec -T postgres \
  psql -U wama -d wama -c 'SELECT current_database(), current_user;'
```

## Finalized measurement sessions

The final-only raw-Protobuf contract is
[`measurement_session.proto`](docs/wama/schema/measurement_session.proto).
The profile-gated exporter uploads immutable artifacts and a canonical,
SHA-256-addressed manifest to `wama-measurement-sessions`, then publishes one
Kafka record keyed by its session ID. It does not infer live windows or emit
lifecycle updates.

`measurement-session-api` materializes that Kafka evidence idempotently into
PostgreSQL and verifies the manifest plus SeaweedFS object metadata before it
lists or streams any artifact. The browser is anonymous, read-only, and has no
Kafka, PostgreSQL, or S3 credentials; it can access artifacts only through the
API proxy. This access model is limited to the trusted local PoC.

Run the complete contract-to-download check after the normal stack is ready:

```sh
scripts/test-measurement-session-flow.sh
```

The script uses a unique final session ID, invokes the exporter, independently
decodes the raw Kafka record, waits for catalog materialization through the
browser's same-origin API path, and verifies the proxied artifact bytes.

## Initialized topic contract

| Topic | Type | Cleanup policy | Purpose |
| --- | --- | --- | --- |
| `LiveMeasurement` | stream | `delete` | `MCCSMeasurementValue` Common Format measurements and derived values |
| `MeasurementSession` | stream | `delete` | Bounded measurement sessions with start, end, and measurement context |
| `Alarm` | stream | `delete` | Alarm records |
| `Export` | stream | `delete` | Records for real-time and file export |
| `Masterdata` | compacted | `compact` | Source masterdata and capabilities |
| `Schema` | compacted | `compact` | Common Format schema definitions |
| `Blobmeta` | compacted | `compact` | SeaweedFS blob pointers and metadata |

All topics have one partition and replication factor one because this is a
single-broker PoC. Automatic topic creation is disabled. No retention period
is set beyond Kafka's broker default because the data-retention decision remains
open.

The `Event` -> `MeasurementSession` topic rename and
`wama-events` -> `wama-measurement-sessions` bucket rename are intentional
clean PoC breaks. A local stack initialized with the old names must be reset
with `docker compose down -v` before it is started again; no dual-publish,
topic bridge, or data migration is provided.

`LiveMeasurement` uses raw Protobuf. `pmu-gateway` serializes
`MCCSMeasurementValue` records directly from
[the canonical schema](docs/wama/schema/rtd_schema.proto); see
[the Common Format contract](docs/wama/02-dataflow-contracts.md). Do not use
JSON console messages as application data on that topic.

`MeasurementSession` also uses raw Protobuf, serialized from
[the finalized-session schema](docs/wama/schema/measurement_session.proto).
Its Kafka payload contains bounded metadata and an integrity-checked manifest
reference only; artifacts remain in SeaweedFS.

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

The Druid Router is exposed without authentication on port 8888 for trusted-LAN
SQL, native-query, supervisor, and web-console access. Druid's Coordinator,
Overlord, Broker, Historical, task runner, and in-container coordination ports
remain Compose-internal. Do not expose this Router outside the trusted PoC
network without adding authentication, TLS, and appropriate authorization.

The measurement-session browser is exposed on port 3002 without authentication
for this trusted PoC. Its catalog API has no host port, and it is the sole path
that can retrieve a SeaweedFS artifact for the browser. Do not expose either
service outside the trusted local network.

The sole Actions runner mounts the host Docker socket and uses Docker socket
automount for workflow job containers. A workflow can therefore control the
host Docker daemon and every container it manages. This is intentional for the
PoC's later build and `docker compose up -d` deployment loop. Use this stack
only on a trusted LAN. Do not use it as a production deployment without adding
appropriate authentication, encryption, authorization, backup, high
availability, runner isolation, and multi-broker durability.
