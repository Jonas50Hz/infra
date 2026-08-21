# WAMA Data Platform PoC

Local Docker Compose backbone for the WAMA proof of concept. It runs one plain
Apache Kafka broker in KRaft combined mode, initializes the WAMA topic
contract, provides Kafka UI to inspect brokers, topics, consumer groups, and
messages, retains an optional legacy PMU fixture for reference, provides Forgejo with one
Actions runner, includes SeaweedFS as authenticated S3-compatible blob storage,
and accepts browser-confirmed bounded measurement-session requests before
materializing them from Druid into integrity-checked Parquet artifacts.
Compacted raw-Protobuf `Blobmeta` results
are projected into PostgreSQL for session, artifact, status, and MRID-coverage
queries; individual samples remain in Druid and SeaweedFS. Grafana dashboards
backed by VictoriaMetrics provide host and Docker container infrastructure
telemetry plus Kafka broker and topic operations metrics, while a Druid-backed
dashboard displays live PMU measurements over time. A persistent single-server
Druid instance directly ingests raw-Protobuf `LiveMeasurement` records for live
SQL queries. A root-owned IEC 60870-5-104 controlled station consumes typed
raw-Protobuf `Export` records and sends monitor-direction values to one control
center connection. An on-demand browser becomes that control center only while
its live page is open and displays the values it receives on the IEC wire.

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
and every asset outside the narrow Forgejo deployment scope. The legacy
`pmu-gateway` fixture is retained for reference but excluded from the default
Compose stack. This checkout is
never added as a Forgejo remote and is never pushed to Forgejo.

[`forgejo-repos/processor-frequency-scale/`](forgejo-repos/processor-frequency-scale/),
[`forgejo-repos/processor-apparent-power/`](forgejo-repos/processor-apparent-power/),
[`forgejo-repos/processor-frequency-iec104-export/`](forgejo-repos/processor-frequency-iec104-export/), and
[`forgejo-repos/processor-lfr-frequency-provision/`](forgejo-repos/processor-lfr-frequency-provision/)
are separate processor-repository seeds. `forgejo-init` automatically creates
one private Forgejo repository per seed and seeds each `main` branch only when
its remote has no refs. An existing nonempty private repository is left
unchanged. Each repository contains one internal processor, its one-service
Compose fragment, deployment tooling, and a Forgejo Actions workflow. The IEC
104 seed maps the fake-PMU frequency directly to a configured `M_ME_NC_1`
`ExportRecord`; it does not implement the full LFR preferred-frequency
algorithm. The LFR seed evaluates configured multi-PMU frequency and voltage
inputs per UTC second and writes its preferred frequency to `LiveMeasurement`;
it does not yet produce IEC 104 export requests. A future gateway may use its
own repository only as an explicit
gateway-deployment test; it does not move the deprecated `pmu-gateway` fixture or any other
infrastructure service out of this checkout. Processor containers connect
through the external `wama-infra` Docker network; they do not include, modify,
or redeploy this Compose project.

[`forgejo-repos/gateway-c37-118-onboarding/`](forgejo-repos/gateway-c37-118-onboarding/)
is the explicit C37.118 gateway-deployment-test seed. A reviewed legacy-v2
source catalog reconciles raw-Protobuf Masterdata records and tombstones to
Kafka, then reconciles one source-scoped adapter per active catalog source. It
does not modify the deprecated `pmu-gateway` fixture or run root Compose services.

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
| [services/pmu-gateway/](services/pmu-gateway/) | `pmu-gateway` (deprecated) | Retained reference fixture, image, source, and tests; excluded from default Compose |
| [services/gateway-dashboard-provisioner/](services/gateway-dashboard-provisioner/) | `gateway-dashboard-provisioner` | Root-owned compacted-Masterdata consumer and generated Grafana gateway dashboards |
| [services/c37-118-simulator/](services/c37-118-simulator/) | `c37-118-simulator` | Profile-gated standalone C37.118 TCP simulator and manual fleet-test tooling |
| [services/iec104-exporter/](services/iec104-exporter/) | `iec104-exporter` | One-way IEC 104 controlled station consuming raw-Protobuf `Export` records |
| [services/iec104-receiver/](services/iec104-receiver/) | `iec104-receiver` | Profile-gated test control center for IEC 104 protocol verification |
| [services/iec104-browser/](services/iec104-browser/) | `iec104-browser` | On-demand browser control center for wire-received IEC 104 values |
| [services/druid/](services/druid/) | `druid` | Persistent single-server Druid image, canonical Protobuf descriptor build, and Router API |
| [services/druid-init/](services/druid-init/) | `druid-init` | Idempotent `LiveMeasurement` Kafka supervisor initializer and tests |
| [services/postgres/](services/postgres/) | `postgres` | Compose fragment, trusted local credentials, and persistent prepared database |
| [services/trino-init/](services/trino-init/) | `trino-init` | Idempotent PostgreSQL reader-role bootstrap for federation |
| [services/trino/](services/trino/) | `trino` | Read-only Druid, Blobmeta, and Iceberg session SQL federation coordinator |
| [services/trino-session-writer/](services/trino-session-writer/) | `trino-session-writer` | Internal-only Iceberg metadata writer for verified session artifacts |
| [services/trino-session-init/](services/trino-session-init/) | `trino-session-init` | One-shot Iceberg schema/table initializer for session artifacts |
| [services/seaweedfs/](services/seaweedfs/) | `seaweedfs` | Compose fragment and SeaweedFS S3/admin configuration |
| [services/measurement-session-api/](services/measurement-session-api/) | `measurement-session-api` | Local browser-confirmed raw-Protobuf MeasurementSession request publisher |
| [services/measurement-session-exporter/](services/measurement-session-exporter/) | `measurement-session-exporter` | Loopback-only read-only CSV download for selected immutable session values |
| [services/measurement-session-processor/](services/measurement-session-processor/) | `measurement-session-processor` | Root-owned Druid-to-Parquet Kafka worker for bounded session requests |
| [services/blobmeta-catalog/](services/blobmeta-catalog/) | `blobmeta-catalog` | Compacted Blobmeta-to-PostgreSQL immutable metadata materializer |
| [services/measurement-session-query-indexer/](services/measurement-session-query-indexer/) | `measurement-session-query-indexer` | Root-owned verified Blobmeta-to-Iceberg query indexer |
| [services/measurement-session-e2e/](services/measurement-session-e2e/) | `measurement-session-e2e` | Profile-gated complete/partial request-to-Blobmeta and Iceberg query-index verifier |
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
directory. Provision each in its own separate processor repository seed.

## Prerequisites

- Docker Engine with Docker Compose v2 or newer
- Host ports `127.0.0.1:29092`, `127.0.0.1:8428`, `8080`, `8085`, `8333`,
  `23646`, `3000`, `3001`, `3003`, `3004`, `3005`, `5432`, `8888`, `2222`, and
  `127.0.0.1:2404` available

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

`kafka`, `druid`, `postgres`, `seaweedfs`, `victoria-metrics`, `grafana`,
`iec104-exporter`, `iec104-browser`, `measurement-session-api`, and
`measurement-session-exporter` should be `healthy`; `kafka-init` and
`druid-init` should have exited with status `0`; and `kafka-ui`,
`kafka-exporter`, `measurement-session-processor`,
`blobmeta-catalog`, `node-exporter`, `cadvisor`, `forgejo`, and
`forgejo-runner` should be running.
`forgejo-init` and `infra-readiness` should have exited with status `0`; the
latter verifies the Kafka contract, service control planes, PostgreSQL,
SeaweedFS S3, Forgejo, monitoring path, and IEC 104 listener. Live PMU traffic
and Druid/Grafana sample queries are opt-in through
`INFRA_READINESS_REQUIRE_LIVE_MEASUREMENT=true` with an external producer.
The profile-gated measurement-session request-flow verifier and IEC 104 test
receiver run only when explicitly invoked.

Stop the stack while preserving Kafka, Druid, PostgreSQL, SeaweedFS, Forgejo,
runner, Grafana, and VictoriaMetrics data:

```sh
docker compose down
```

Start it again with `docker compose up -d`. Kafka topic initialization and
Forgejo runner registration and processor repository seeding are idempotent.

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

Run the complete one-way IEC 104 path:

```sh
scripts/test-iec104-export.sh
```

The script starts the root-owned exporter, connects a profile-gated test control
center using `STARTDT` only, publishes three unique raw-Protobuf `Export`
records, and proves receipt of single-point, double-point, and short-float
monitor values with their COT, information-object address, quality, and value.
It then sends a raw general-interrogation I-frame and requires the exporter to
close the connection without an application response. The script stops the IEC
104 browser while its profile-gated test control center owns the one IEC
connection, then restores the browser idle afterward.

Run the browser wire-to-page validation:

```sh
scripts/test-iec104-browser.sh
```

The script opens the browser's transient WebSocket control center, publishes a
unique three-ASDU fixture through Kafka, proves the live page stream receives
each typed value, then requires the browser to release IEC 104 after the stream
closes.

Open the live IEC 104 monitor at `http://localhost:3003`. Opening the page
starts its read-only control-center connection and shows only values received on
that IEC connection. Closing the final page disconnects it and discards the
page's value list. The browser and the profile-gated receiver cannot be active
at the same time because the exporter permits one control center.

## Forgejo Actions

`forgejo-init` creates the configured administrator, bootstraps the separate
`<owner>/gateway-c37-118-onboarding` repository, and initializes the approved
processor registry. Its first run registers the four tracked processor seeds;
later runs reconcile only the registry, so a folder cannot grant itself
deployment privileges. Each registered processor owns exactly one marked child
root under `/var/lib/wama-processors`; onboarding retains its separate gateway
root. Bootstrap skips seeding without modifying any existing repository that
already has refs, and it fails without mutation if an existing repository is
not private. Its generated runner credentials remain in the
`forgejo-runner-data` volume.

The root-only lifecycle interface validates a manifest against the reviewed
input and output catalogs before it can create repository connections or a
deployment root:

```sh
scripts/wama-processor-admin.sh status
scripts/wama-processor-admin.sh register processor-example
scripts/wama-processor-admin.sh deploy-existing processor-example
scripts/wama-processor-admin.sh unregister processor-example
```

After bootstrap, clone the individual managed repository you intend to work on.
These commands must never be run from this infrastructure checkout:

```sh
git clone "https://<forgejo-host>/<owner>/processor-frequency-scale.git"
cd processor-frequency-scale
```

For the existing private `gateway-c37-118-onboarding` checkout, bootstrap
creates a restricted non-admin collaborator automatically. Install its local
credential without creating or copying a token by hand:

```sh
sh scripts/configure-forgejo-gateway-onboarding-agent.sh \
  --checkout ../gateway-c37-118-onboarding
```

The installer accepts only that external checkout, preserves its `origin`, and
rejects this parent checkout and `forgejo-repos/gateway-c37-118-onboarding`.
Normal Git pushes then use the local credential helper. For Forgejo REST calls,
run the command through
`scripts/with-forgejo-gateway-onboarding-agent.sh --checkout
../gateway-c37-118-onboarding -- <command>`; the generated token is exported
only to that child command. Re-run the installer after `docker compose down -v`.

Standard processors are authored through `processor.yaml`, `calculation.py`,
and `cases.yaml`; their generated workflows verify the generated-file lock,
catalog/approval evidence, engineering cases, deployment guard, and container
test target on pull requests. A trusted `main` push then publishes only its one
processor image, synchronizes only its checkout to its own deployment child
root, verifies the OCI revision and running container state, and deploys only
that service. A gateway may use Forgejo only in a deliberately declared
gateway-deployment test; that exception must not deploy, modify, or take
ownership of the deprecated `pmu-gateway` fixture or any root infrastructure
service.

The `gateway-c37-118-onboarding` workflow follows the same trusted
`validate -> publish -> deploy` sequence. Its deploy step runs
`masterdata-publisher` once through `docker compose run --rm`, then uses the
marker-owned deployment guard to reconcile only generated legacy-v2 source
adapters. It cannot control the deprecated root `pmu-gateway` fixture, root Compose project, or
any adapter absent from its approved catalog.

Validate the canonical Masterdata contract, onboarding seed, isolated Compose
project, Forgejo bootstrap guard, and onboarding credential bridge together:

```sh
sh scripts/test-masterdata-onboarding.sh
```

Each managed repository has distinct repository-scoped CI and deployment runner
connections, all handled by the same capacity-one runner daemon. The deployment
connection runs in the runner container with the host Docker socket and its
matching deployment root mounted at the same path. This is trusted local-PoC
access: managed-repository workflow authors can control the Docker host. Do not
expose this runner to untrusted repositories, users, or production workloads.

Create or adapt processors only from the instructions in the individual
[frequency-scale README](forgejo-repos/processor-frequency-scale/README.md) or
[apparent-power README](forgejo-repos/processor-apparent-power/README.md) or
[frequency IEC 104 export README](forgejo-repos/processor-frequency-iec104-export/README.md) or
[LFR frequency provision README](forgejo-repos/processor-lfr-frequency-provision/README.md), or
[C37.118 onboarding README](forgejo-repos/gateway-c37-118-onboarding/README.md).
Each repository owns its Python code, test suite, and app-local Compose fragment.

## Deprecated PMU fixture

`pmu-gateway` loads
[services/pmu-gateway/messages.yaml](services/pmu-gateway/messages.yaml) once
at startup and continuously publishes its configured scalar PMU measurements in
fixture order. It is excluded from the default Compose stack and retained only
as a reference fixture. The default fixture contains three-phase voltage and
current, frequency, and ROCOF records.

Each entry needs an `mrid` and exactly one typed Common Format value. Supported
value names are `double_value`, `int_value`, `uint_value`, `bool_value`,
`string_value`, and `timestamp_value`; see the service
[configuration reference](services/pmu-gateway/README.md). The gateway creates
Kafka, gateway, and MCCS timestamps at send time. It can optionally derive a
field timestamp from `field_timestamp_offset_ms`.

The default `double_value` signals use small bounded `value_jitter` amplitudes,
so each publish cycle produces realistic variation around its nominal PMU
value. Custom fixture values remain fixed unless their individual entries opt
into jitter.

To run the retained fixture explicitly, include its service fragment and then
recreate the gateway:

```sh
docker compose -f docker-compose.yml -f services/pmu-gateway/compose.yaml up -d --build pmu-gateway
```

To select a different fixture for startup, use an absolute host path:

```sh
PMU_GATEWAY_CONFIG_SOURCE="$PWD/my-pmu-messages.yaml" \
  PMU_GATEWAY_PUBLISH_INTERVAL_MS=250 \
  docker compose -f docker-compose.yml -f services/pmu-gateway/compose.yaml \
  up -d --build pmu-gateway
```

## C37.118 Simulator

The profile-gated [C37.118 simulator](services/c37-118-simulator/) is a
standalone C37.118.2-2011 V2 and C37.118.2-2024 V3 TCP source and protocol test
service. It neither implements nor validates a gateway, and it has no Kafka,
Common Format, or Druid dependency. V2 performs HDR -> CFG-1 -> CFG-2 -> start
-> periodic data -> stop; V3 performs capability -> stream configuration ->
start -> periodic data -> stop.

```sh
docker compose --profile c37-118 up -d --build c37-118-simulator
```

Its regular Docker test target covers the C37.118 frame/profile/server/probe
slice. The 25-PMU and 100-PMU tests are separately armed manual soaks and never
run as part of normal infrastructure lifecycle validation.

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
| Measurement-session request | `http://localhost:3004` |
| Measurement-session CSV export | `http://localhost:3005` |
| Measurement session dashboard | `http://<host-ip>:3001/d/wama-measurement-sessions/wama-measurement-sessions` |
| Trino SQL query API and web UI | `http://<host-ip>:8085` |
| Druid Router API and web console | `http://<host-ip>:8888` |
| VictoriaMetrics API and VMUI | `http://127.0.0.1:8428` |
| IEC 104 controlled station | `tcp://127.0.0.1:2404` |
| IEC 104 live monitor | `http://localhost:3003` |
| IEC 104 live monitor | `http://localhost:3003` |

Kafka UI exposes topic contents, partitions, consumer groups, messages, and
broker metadata. Its port is exposed on all host interfaces so it can be opened
from the LAN. Forgejo HTTP and SSH are likewise exposed on all host interfaces.
Grafana and PostgreSQL are LAN-accessible on ports 3001 and 5432 respectively;
VictoriaMetrics is available only from the Docker host, and node-exporter plus
cAdvisor expose no host ports. Druid exposes only its Router on port 8888 to all
host interfaces; its Coordinator, Overlord, Broker, Historical, task runner,
and in-container coordination endpoints have no host mappings.

Trino is LAN-accessible on port 8085 without authentication in this trusted
PoC. Its global read-only access control and dedicated PostgreSQL reader roles
permit Druid, Blobmeta metadata, and registered measurement-session artifact
queries. The separate `trino-session-writer` coordinator has no host port and
is reserved for root-owned registration of verified canonical Parquet files.

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
  ROCOF values as separate unit-safe trends plus recent timestamp evidence. Its
  MRID selector and session link open the loopback-only confirmation page with
  the current selected range and measurements.
- **WAMA Measurement Sessions**: selected immutable session values through the
  internal read-only Trino datasource, with Blobmeta evidence and MRID coverage.
  Its **Export CSV** dashboard action downloads the current selection through
  the loopback-only fixed-query exporter.

The root-owned `gateway-dashboard-provisioner` also consumes compacted
`Masterdata` and provisions the **WAMA Gateways** folder:

- **WAMA Gateway Fleet**: the active catalog sources and links to their pages.
- **WAMA Gateway: <source>**: source provenance, endpoint metadata, unit-safe
  Druid live-value trends, freshness, and latest valid records.

Gateway dashboard membership follows active Masterdata records, including
tombstones; it is not evidence of a running adapter or a substitute for gateway
deployment health. The static PMU fixture dashboard remains available before
any catalog source is published.

No alert rules, contact points, or notification delivery are provisioned in
this PoC slice. Set `GRAFANA_ROOT_URL=http://<host-ip>:3001/` when Grafana must
generate external URLs for a LAN address; completed measurement-session links
use the same browser-reachable origin.

The Druid and Trino plugins are pinned in the local Grafana image. Druid remains
the live Common Format query source, while Trino serves only registered immutable
measurement-session artifacts and Blobmeta metadata. VictoriaMetrics remains
infrastructure-only; alerting and broader cross-session analytics remain deferred.

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
persists them in the `postgres-data` Compose volume. `blobmeta-catalog` owns the
immutable `blobmeta_catalog` schema and materializes compacted `Blobmeta`
records after validating their raw-Protobuf evidence. Kafka remains the source
of truth. A future Kafka Connect mirror for `Masterdata` and `Schema` remains
separate from this session path.

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

## Measurement sessions

[`measurement_session.proto`](docs/wama/schema/measurement_session.proto)
defines a raw-Protobuf `MeasurementSessionRequest` keyed by its canonical UUID.
It contains a requested timestamp, a bounded half-open `[started_at, ended_at)`
interval, sorted unique MRIDs, and bounded context metadata. The root-owned
processor validates the request, queries Druid, writes one long-form typed
Parquet artifact to `wama-measurement-sessions`, stores an immutable replay
receipt, then publishes keyed raw-Protobuf
[`Blobmeta`](docs/wama/schema/blobmeta.proto).

`Blobmeta` identifies the artifact, Parquet schema version, SHA-256, byte size,
request digest, completion status, and per-MRID row coverage. `COMPLETE` means
each requested MRID has data; `PARTIAL` records missing MRIDs; bounded validation
failures produce `REJECTED` evidence. `blobmeta-catalog` projects only this
metadata to PostgreSQL; it never copies individual measurements there.

V2 artifacts include immutable `blob_id` and `session_id` in every Parquet row.
`measurement-session-query-indexer` verifies object metadata and bytes, the
Parquet v2 footer/schema/field IDs, row identities, count, and MRID coverage. It
then registers the exact `measurements.parquet` object URI through the internal
writer in `sessions.wama.measurement_values`; it never scans the directory that
also holds the replay receipt. The host-exposed Trino coordinator and Grafana
datasource remain read-only.

`measurement-session-exporter` is a loopback-only download surface for the
current Grafana session selection. It queries the public read-only Trino
coordinator with a fixed statement over one canonical `blob_id`, selected MRIDs,
value types, and time range, then streams the chart values as CSV. It has no
direct object-store, PostgreSQL, Kafka, Druid, or internal Trino-writer access.

Run the complete request-to-Blobmeta check after the normal stack is ready:

```sh
scripts/test-measurement-session-flow.sh
```

The script submits unique complete and partial requests, independently validates
their raw Kafka Blobmeta records, waits for PostgreSQL materialization, and
verifies SeaweedFS hashes plus Parquet row coverage. It then requires each exact
artifact to appear in `session_query_index.registrations`, be queryable with the
same Blobmeta identity and row count through
`sessions.wama.measurement_values` on public read-only Trino, and return a
selected-session frame through Grafana's Trino datasource.
Its query-side assertions route through public Trino; direct SeaweedFS reads
remain only for immutable-object hash, schema, and row-identity verification.

## Initialized topic contract

| Topic | Type | Cleanup policy | Purpose |
| --- | --- | --- | --- |
| `LiveMeasurement` | stream | `delete` | `MCCSMeasurementValue` Common Format measurements and derived values |
| `MeasurementSession` | stream | `delete` | Raw-Protobuf bounded historical session requests |
| `Alarm` | stream | `delete` | Alarm records |
| `Export` | stream | `delete` | Records for real-time and file export |
| `Masterdata` | compacted | `compact` | Raw-Protobuf C37.118 source, endpoint, and signal-to-MRID masterdata |
| `Schema` | compacted | `compact` | Common Format schema definitions |
| `Blobmeta` | compacted | `compact` | Raw-Protobuf immutable Parquet pointers, status, and MRID coverage |

`MeasurementSession` and `Blobmeta` default to 12 partitions so their root
worker pools can scale; override them with `MEASUREMENT_SESSION_TOPIC_PARTITIONS`
and `BLOBMETA_TOPIC_PARTITIONS`. All other topics have one partition, and every
topic has one replica because this is a single-broker PoC. Automatic topic
creation is disabled. No retention period is set beyond Kafka's broker default
because the data-retention decision remains open.

The request/Blobmeta contracts, v2 queryable Parquet artifact, and 12-partition
worker topics replace the old finalized-session exporter, catalog API, browser,
and manifest flow. The current dashboard provides only the bounded selected
session CSV download described above. This is an intentional clean PoC break: run
`docker compose down -v` before starting an existing local stack. The default
indexer starts at `earliest` and fails closed on pre-v2 evidence; no dual-publish,
bridge, backfill, or migration is provided.

`LiveMeasurement` uses raw Protobuf. `pmu-gateway` serializes
`MCCSMeasurementValue` records directly from
[the canonical schema](docs/wama/schema/rtd_schema.proto); see
[the Common Format contract](docs/wama/02-dataflow-contracts.md). Do not use
JSON console messages as application data on that topic.

`MeasurementSession` uses raw Protobuf, serialized from
[the request schema](docs/wama/schema/measurement_session.proto). Its Kafka
payload carries only the bounded extraction command. `Blobmeta` uses raw
Protobuf, serialized from [the result schema](docs/wama/schema/blobmeta.proto),
and is keyed by immutable `blob_id`; Parquet bytes remain in SeaweedFS.

`Export` uses raw Protobuf, serialized from
[the IEC 104 export schema](docs/wama/schema/iec104_export.proto). The current
root-owned exporter supports outbound `M_SP_NA_1`, `M_DP_NA_1`, and `M_ME_NC_1`
monitor values only. A future processor may produce this contract from its own
Forgejo repository, but no export-producing processor is part of this checkout.

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

The sole Actions runner mounts the host Docker socket and uses Docker socket
automount for workflow job containers. A workflow can therefore control the
host Docker daemon and every container it manages. This is intentional for the
PoC's later build and `docker compose up -d` deployment loop. Use this stack
only on a trusted LAN. Do not use it as a production deployment without adding
appropriate authentication, encryption, authorization, backup, high
availability, runner isolation, and multi-broker durability.
