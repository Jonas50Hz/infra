# WAMA Kafka PoC

Local Docker Compose backbone for the WAMA proof of concept. It runs one plain
Apache Kafka broker in KRaft combined mode, initializes the WAMA topic
contract, and provides Kafka UI to inspect brokers, topics, consumer groups,
and messages.

This is the Phase 1 backbone only. It deliberately does not include ZooKeeper,
Confluent components, a Schema Registry, a producer, a Quixstreams processor,
or Kubernetes artifacts.

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

Add a matching directory whenever a Compose service is added. Keep the root
Compose file limited to includes and shared resources; place each service's
definition, Dockerfiles, configuration, scripts, and service documentation in
that service directory.

## Prerequisites

- Docker Engine with Docker Compose v2 or newer
- Port `127.0.0.1:29092` and port `8080` on all host interfaces available

## Start and stop

Start the broker, initialize the topics, and start Kafka UI:

```sh
docker compose up -d
```

Check the service state:

```sh
docker compose ps
docker compose logs kafka-init
```

`kafka` should be `healthy`, `kafka-init` should have exited with status `0`,
and `kafka-ui` should be running.

Stop the stack while preserving Kafka data:

```sh
docker compose down
```

Start it again with `docker compose up -d`. Topic initialization is idempotent
and reapplies the expected cleanup policy to the WAMA topics.

To remove all local Kafka data and begin with an empty broker, stop the stack
and remove its Compose-managed volume:

```sh
docker compose down -v
```

## Access

| Purpose | Address |
| --- | --- |
| Kafka UI | `http://<host-ip>:8080` (including <http://127.0.0.1:8080>) |
| Kafka from the host | `localhost:29092` |
| Kafka from another Compose service | `kafka:9092` |

Kafka UI exposes topic contents, partitions, consumer groups, messages, and
broker metadata. Its port is exposed on all host interfaces so it can be opened
from the LAN.

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

`LiveMeasurement` uses raw Protobuf when an application producer is added; see
[the Common Format contract](docs/wama/02-dataflow-contracts.md) and
[the canonical schema](docs/wama/schema/rtd_schema.proto). Do not use JSON
console messages as application data on that topic.

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

## Network exposure and security model

The Kafka broker remains bound to `127.0.0.1:29092`; Compose services use the
internal `kafka:9092` endpoint. Kafka UI is intentionally exposed on
`0.0.0.0:8080` and has unrestricted access to the local broker without
authentication. Only use it on a trusted LAN and restrict inbound port 8080
with the host firewall if necessary. Traffic within the Compose network is
plaintext. Do not use this stack as a production deployment without adding
authentication, encryption, authorization, and multi-broker durability.
