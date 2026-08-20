# Infrastructure readiness

`infra-readiness` is a one-shot Compose service. It exits with status `0` only
after the current infrastructure is ready for application processors. It has no
host port and no persistent state.

The service verifies the WAMA Kafka topic contract, consumes a fresh raw
`MCCSMeasurementValue` from `LiveMeasurement` without committing an offset,
checks the Druid Router and `live_measurements` supervisor, and queries the PMU
frequency fixture through Druid SQL to require matching `__time` and
`timestamp_mccs`. It also checks PostgreSQL connectivity and identity, and
performs signed temporary S3 put/get/delete operations in both SeaweedFS
buckets. It checks both private seeded Forgejo processor repositories and their
separate CI/deployment Actions runner connections, Kafka UI, Grafana
provisioning, and VictoriaMetrics scrape health including Kafka exporter metrics
for all WAMA topics. Grafana readiness requires the VictoriaMetrics datasource,
the internal Druid datasource, the **WAMA Measurements / WAMA PMU Live
Measurements** dashboard, and a Grafana datasource query returning the expected
PMU frequency value. It also checks the IEC 104 browser HTTP health and accepts
its idle or viewer-owned status without opening an IEC control-center connection.

The probe deliberately does not require PostgreSQL to have no application
tables. The measurement-session catalog is an app-owned schema and is verified
by its own contract-to-download test, not by this infrastructure-only gate.

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
override those limits. Set `PMU_GATEWAY_EXPECTED_MRID_PREFIX` when a custom PMU
fixture uses a different MRID namespace. `DRUID_ROUTER_URL`,
`DRUID_SUPERVISOR_ID`, `DRUID_DATASOURCE`, `DRUID_EXPECTED_MRID`, and
`DRUID_EXPECTED_DOUBLE_VALUE` customize the Druid evidence check.