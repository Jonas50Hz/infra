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

## Memory budget

The container runs embedded ZooKeeper plus Coordinator/Overlord, Historical,
Broker, Router, and one local Kafka ingestion task while `live_measurements` is
active. The PoC keeps the Druid service JVMs deliberately small:

| Process | Initial heap | Maximum heap | Direct memory |
| --- | ---: | ---: | ---: |
| Coordinator/Overlord | 128 MiB | 256 MiB | 128 MiB |
| Historical | 96 MiB | 160 MiB | 64 MiB |
| Broker | 64 MiB | 96 MiB | 64 MiB |
| Router | 64 MiB | 96 MiB | 64 MiB |
| Local Kafka task | 256 MiB | 256 MiB | 300 MiB |

Measure a healthy, initialized instance without changing its state:

```sh
services/druid/scripts/measure-memory.sh
```

The sampler requires Docker cgroup accounting and reports process RSS plus
warm-up, peak, and final container cgroup memory. Optional arguments set the
sample duration and warm-up point in seconds:

```sh
services/druid/scripts/measure-memory.sh 120 60
```

There is intentionally no Compose `mem_limit`. Set one only after repeated
post-warm-up measurements, with 25-30% headroom above the observed peak.

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