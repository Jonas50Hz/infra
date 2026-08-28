# Infrastructure readiness

`infra-readiness` is a one-shot Compose service. It exits with status `0` only
after the current infrastructure is ready for application processors. It has no
host port and no persistent state.

The service verifies the WAMA Kafka topic contract. When live-measurement
checking is enabled, it consumes a fresh raw `MCCSMeasurementValue` from
`LiveMeasurement` without committing an offset, checks the Druid Router and
`live_measurements` supervisor, and queries the PMU frequency data through Druid
SQL to require matching `__time` and `timestamp_mccs`. It checks read-only
Trino catalog visibility for Druid,
Blobmeta, and the initialized `sessions.wama.measurement_values` Iceberg table.
It also checks PostgreSQL connectivity and identity, and
performs signed temporary S3 put/get/delete operations in both SeaweedFS
buckets. It checks every configured private seeded Forgejo managed repository
and its separate CI/deployment Actions runner connections, Kafka UI, Grafana
provisioning, and VictoriaMetrics scrape health including Kafka exporter metrics
for all WAMA topics. Grafana readiness requires the VictoriaMetrics datasource,
the internal Druid datasource, the read-only internal Trino datasource, the
**WAMA Measurements / WAMA Measurement Sessions** dashboard, and a Trino metadata query proving the
initialized Iceberg session table is visible. It also requires the provisioned
**WAMA Gateways / WAMA Gateway Fleet** entry point, but does not require an
active catalog source. It checks the local measurement-session API HTTP health
and CSV exporter HTTP health, plus Grafana's selected-MRID session and CSV
download links without publishing a request or exporting data. It checks the
IEC 104 browser HTTP health and accepts its persistent active status, including
zero UI viewers. It does not open a second IEC control-center connection.

The probe deliberately does not require PostgreSQL to have no application
tables. The Blobmeta catalog is an app-owned schema and is verified by its own
request-to-Blobmeta test, not by this infrastructure-only gate.

The temporary S3 objects are deleted before a successful probe exits. The
consumer group is unique and auto-commit is disabled, so the probe does not
change application consumer offsets.

Run the normal stack with `docker compose up -d`, then inspect the successful
one-shot result:

```sh
docker compose ps --all infra-readiness
docker compose logs infra-readiness
```

Rerun only the readiness gate after changing infrastructure configuration:

```sh
docker compose up -d --force-recreate infra-readiness
docker compose wait infra-readiness
```

It retries for up to 180 seconds by default to allow Grafana provisioning and
VictoriaMetrics' 15-second scrape interval to settle. Set
`INFRA_READINESS_TIMEOUT_SECONDS`, `INFRA_READINESS_RETRY_INTERVAL_SECONDS`, or
`INFRA_READINESS_KAFKA_CONSUME_TIMEOUT_SECONDS` in the command environment to
override those limits. Set `INFRA_READINESS_REQUIRE_LIVE_MEASUREMENT=true` to
require an external `LiveMeasurement` producer and enable the Druid, Trino, and
Grafana PMU data probes. Set `PMU_EXPECTED_MRID_PREFIX` when that producer uses
a different MRID namespace. `DRUID_ROUTER_URL`,
`DRUID_SUPERVISOR_ID`, `DRUID_DATASOURCE`, `DRUID_EXPECTED_MRID`, and
`DRUID_EXPECTED_DOUBLE_VALUE` customize the Druid evidence check.