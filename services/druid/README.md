# Druid

`druid` is the root-owned Apache Druid 37 single-server live-measurement store.
It is not a Kafka broker and does not add a ZooKeeper Compose service: Kafka
remains the plain single-broker KRaft service. Druid's single-server coordination
runs inside this container only.

The image generates a Protobuf descriptor from the canonical
[`docs/wama/schema/rtd_schema.proto`](../../docs/wama/schema/rtd_schema.proto)
during its multi-stage build. It loads `druid-kafka-indexing-service` and
`druid-protobuf-extensions`; no Schema Registry is used.

State, metadata, task logs, and segments persist in the shared `druid-data`
volume mounted at `/opt/druid/var`. Only the Router API is published, on
`0.0.0.0:8888`; all other Druid endpoints remain Compose-internal.
The Compose health check requires the internal Coordinator/Overlord, Broker,
Historical, Router, and Router management forwarding to be ready before
`druid-init` starts.

Build the descriptor-focused test target:

```sh
docker build --target test --file services/druid/Dockerfile .
```

Run the complete Kafka, PMU, supervisor, and SQL query check:

```sh
scripts/test-druid-ingestion.sh
```

No retention, deletion, compaction, rollup, or aggregation policy is configured
in this PoC. Grafana provisions **WAMA Measurements / WAMA PMU Live
Measurements** over the internal Druid Router; alert rules remain deferred.