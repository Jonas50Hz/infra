# Infrastructure readiness

`infra-readiness` is a one-shot Compose service. It exits with status `0` only
after the current infrastructure is ready for application processors. It has no
host port and no persistent state.

The service verifies the WAMA Kafka topic contract, consumes a fresh raw
`MCCSMeasurementValue` from `LiveMeasurement` without committing an offset,
checks the empty PostgreSQL target, and performs signed temporary S3 put/get/
delete operations in both SeaweedFS buckets. It also checks Forgejo's private
application repository and Actions runner, Kafka UI, Grafana provisioning, and
VictoriaMetrics scrape health including Kafka exporter metrics for all WAMA
topics.

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
fixture uses a different MRID namespace.