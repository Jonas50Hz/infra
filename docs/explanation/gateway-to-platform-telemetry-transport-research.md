(exp_gateway_to_platform_telemetry_transport_research)=

```{meta}
:description: Research framing proven outbound-only transport options from a WAMA gateway zone to the platform.
```

# Gateway-to-platform telemetry transport research

This Explanation compares established transport and ingestion directions for
telemetry sent by a deployed PMU gateway in a safe IT zone. It is research, not
an implementation plan, product decision, capacity claim, or change to the
current WAMA proof of concept.

## Scope and central framing

The current PoC remains Kafka in KRaft mode with Druid directly ingesting raw
Protobuf `LiveMeasurement` records, as defined by the
[WAMA data-flow contract](../reference/wama-data-flow-contracts.md). Nothing in
this note changes that path.

Here, *egress-only* means that the gateway initiates an mTLS connection to an
allow-listed platform destination. The platform does not initiate a connection
or run a management listener in the gateway zone. This is not a literal
unidirectional protocol: after the gateway opens the connection, it can receive
TLS handshake messages, acknowledgments, and flow-control messages on that same
connection. That policy is materially different from the physical-boundary
assumption in the existing
[diode-first live-data architecture research](diode-first-live-data-architecture-research.md).
This note corrects that framing for this organizational constraint without
changing or deleting the physical-diode research. If a physical diode becomes a
real requirement later, every option here must be reevaluated against the
appliance's supported application profiles.

Network access was unavailable while this research was written. The primary
URLs below are unverified verification targets, not live-verified vendor claims.
Exact feature, version, edition, deployment, and support-matrix evidence remains
required before a decision.

## Existing WAMA evidence

The candidate transport must preserve the present contract rather than flatten
it into generic metrics or anonymous rows.

| Current fact | Implication for a future ingress |
|---|---|
| [`MCCSMeasurementValue`](../wama/schema/rtd_schema.proto) is raw Protobuf with an MRID, typed scalar `oneof`, field/gateway/MCCS timestamps, and optional quality fields. | A transport may carry the existing payload directly, or a maintained connector may map it without losing type or optional-quality presence. |
| The [gateway runtime](../../forgejo-repos/gateway-c37-118/src/gateway_c37_118/gateway_runtime.py) waits for Kafka delivery acknowledgment; its producer uses `acks="all"` and retries. Druid then natively ingests raw-Protobuf records from Kafka. | The current path already has acknowledgment, retry, and ingest responsibilities. A replacement must make its corresponding receipt and recovery boundary explicit. |
| The schema has no stable domain event-ID field. The [large-scale storage research](large-scale-time-series-storage-research.md) identifies event and replay identity as future contract work. | Broker offsets and database deduplication keys cannot be assumed to identify the source event. |
| The [data-flow contract](../reference/wama-data-flow-contracts.md) and [LFR reference](../reference/lfr-frequency-provision.md) say that raw C37 `STAT` and complete time-quality evidence are not retained by the present normalized record. | A future audit requirement needs a retained raw/source-status path or an explicitly declared limitation; transport alone cannot reconstruct it. |
| The [measurement-session processor](../../services/measurement-session-processor/README.md) turns bounded requests into immutable Parquet and `Blobmeta` evidence. | Changing live ingress does not automatically replace the session, metadata, artifact, or read-model workflows. |

The 100,000-normalized-scalar-per-second normal workload and
125,000-record-per-second burst in the
[large-scale storage research](large-scale-time-series-storage-research.md)
are proposed benchmarks, not demonstrated capacities of WAMA or any candidate.

## Ingress contract before product choice

The ingress contract is non-negotiable regardless of broker, database, or
receiver:

- The gateway initiates mTLS only to an approved destination allow list; no
  core-initiated connection or gateway-zone management listener is permitted.
- The client identity has a publish-only ACL scoped to its authorized source and
  destination namespace.
- The sender and receiver enforce bounded in-flight work and backpressure.
- The design declares a stale-data policy: expiry, marking, rejection, or a
  bounded recovery rule.
- It supplies a durable sender spool or explicitly declares the permitted gaps,
  loss evidence, and recovery point objective.
- It defines idempotent replay through a stable domain event identity. Broker
  offsets and database deduplication are transport or storage evidence, not a
  domain event identity.
- It preserves source timestamps and quality, including the absence of optional
  quality fields. Any raw C37 audit requirement is separately retained.

No bespoke WAMA transport needs to be invented. The preferred shape carries the
existing Common Format payload through a product-supported protocol or connector
whose receipt, retry, authorization, and recovery behavior can be tested.

## Brokered transport candidates

| Candidate | Why it fits | Boundary and migration consequence |
|---|---|---|
| MQTT 5 | The strongest lightweight edge-ingress candidate. It can run over mTLS, carry raw Protobuf payloads, and offers QoS 1 compared to QoS 0, message expiry, and shared subscriptions. QoS 1 needs duplicate-safe consumers; QoS 0 is an explicit loss trade-off. | Sparkplug B is not the default because it changes WAMA payload and lifecycle assumptions. MQTT still needs an ingestion consumer, event identity, sender spool, and a canonical store. |
| NATS JetStream | A compact option for durable core fan-out and replay while retaining raw Protobuf messages. | Core NATS alone is insufficient for canonical data because it is not the durable replay boundary. JetStream would require migration of processors and the store path from the current Kafka/Druid model. |
| RabbitMQ Streams and AMQP 1.0/Artemis | Conditionally useful where an approved RabbitMQ or AMQP estate already exists. A selected broker can provide durable streams or messages, TTL, and backpressure behavior. | Verify those features for the exact broker, version, and configuration. It is a migration from the Kafka/Druid path, not a transparent bridge. |
| Apache Pulsar | Capable of durable messaging, replay, and multi-tenant platform patterns. | Operationally heavy for this problem; consider it only for a concrete platform-scale or tenancy reason, not as a generic replacement. |
| Redpanda or Kafka baseline | The lowest-migration comparison point because it preserves the current Kafka model. | It does not meet a goal of replacing the Kafka model, and it does not replace Druid. It is a baseline, not a new transport direction. |

## Direct-ingest and storage-coupled candidates

| Candidate | Why it fits | Boundary and migration consequence |
|---|---|---|
| ClickHouse HTTP batch inserts or native protocol | The strongest no-broker candidate when one canonical store is desired. HTTP batch inserts can use mTLS/HTTPS where the selected deployment supports it; the native protocol is another supported client path. | The sender still needs durable spool/retry, stable event identity, and duplicate handling. Fan-out moves after persistence, so it is not independent live-consumer replay. |
| InfluxDB Line Protocol, QuestDB ILP, or TDengine direct protocols | Conditional, store-specific alternatives when their selected protocol and operating model fit the estate. | Each needs a schema mapping, replay, high-availability, retention, and workload benchmark. Never expose a database blindly; ingress and authentication must be approved. |
| HTTPS batch ingest or gRPC streaming | Standard outbound transports that naturally fit gateway-initiated mTLS and flow control. | They are no-custom-protocol options only when a maintained product receiver defines durable receipt and idempotency. Creating an application-specific endpoint recreates custom development. CloudEvents is an optional existing standard envelope, not a required WAMA protocol. |
| OpenTelemetry Protocol (OTLP) | Appropriate for gateway health, lag, loss, connection state, and operational telemetry. | It is not the canonical transport for Common Format PMU records. |
| Apache Arrow Flight | Useful as a standardized data-exchange RPC in an analytics environment. | It is not, by itself, a durable ingest, retention, or fan-out solution. |
| Industrial historian or edge collector | Strong where operational historian tooling and a vendor-supported source/ingest mapping are the priority. | It must prove Common Format semantics, recovery evidence, and bounded session extraction rather than assuming that tag history meets WAMA requirements. |

## Candidate architectures, not decisions

These practical architectures are candidates only. They make no throughput or
capacity promise.

| Candidate architecture | Best fit when | Principal question to prove |
|---|---|---|
| A. MQTT 5 -> ingestion consumer -> ClickHouse or another canonical store | Lightweight edge ingress matters and streams or processor fan-out are not the primary requirement. | Can the consumer preserve Common Format semantics while giving durable receipt, bounded replay, and queryable event identity? |
| B. NATS JetStream -> processors and store | Durable, independent, replayable fan-out is meant to replace Kafka. | Can JetStream and migrated processors/store provide the operational and session evidence now split across Kafka, Druid, and artifact workflows? |
| C. Direct ClickHouse HTTP -> ClickHouse plus Parquet/Iceberg archive | Exactly one canonical store is desired and independent live-consumer replay is acceptable to forgo. | Can gateway-side spooling, idempotent writes, retention, latest/trend queries, and archive/session evidence meet the required recovery behavior? |

## Transport and database selection stay separate

Gateway communication decides how an egress-only connection is authenticated,
authorized, flow-controlled, recovered, and audited. Database selection decides
how typed records are retained, queried, replicated, and extracted. Those are
related but separate decisions; the
[large-scale time-series storage research](large-scale-time-series-storage-research.md)
is the existing comparison frame for the latter.

Even if live transport changes, `MeasurementSession`, `Blobmeta`, immutable
artifact, metadata projection, and read-model workflows still need durable job
and evidence choices. Their current Kafka contracts do not disappear
automatically; the [data-flow contract](../reference/wama-data-flow-contracts.md)
and [session processor](../../services/measurement-session-processor/README.md)
show the responsibilities that a replacement must deliberately retain or
redesign.

## Selection questions and proof plan

Selection starts with these questions:

- What egress firewall, TLS, mTLS client-certificate, destination allow-list,
  and publish-only ACL support is available in the gateway zone?
- What loss behavior and recovery point objective are acceptable during an
  outage, full spool, or expired measurement?
- Must raw C37 `STAT` and time-quality evidence be auditable, and where will it
  be retained?
- Is independent fan-out/replay required, or is persistence-first fan-out
  acceptable?
- What session and read model must remain available after live retention,
  including immutable evidence and bounded extraction?
- What retention, high-availability, recovery, and gateway-to-store rate must
  be proven for the selected deployment?

A vendor-neutral proof should demonstrate outbound mTLS and authorization,
broker or store restart, a bounded outage, retry and duplicate behavior,
stale-data expiry, and latest/trend queries. It should then exercise the
existing proposed workload: 24 hours at 100,000 normalized scalar records per
second followed by the 125,000-record-per-second burst, recording completeness,
recovery, and resource behavior. Those tests establish evidence for a selected
configuration; they do not grant any product a universal capacity figure.

## Primary-source verification targets

These are official primary-source targets for offline research only. They were
not live-verified here and must be checked against the selected version,
edition, deployment, and support contract.

- [OASIS MQTT 5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html)
- [NATS Core](https://docs.nats.io/nats-concepts/what-is-nats) and [NATS JetStream](https://docs.nats.io/nats-concepts/jetstream)
- [RabbitMQ Streams](https://www.rabbitmq.com/docs/streams)
- [OASIS AMQP 1.0](https://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-overview-v1.0-os.html) and [Apache ActiveMQ Artemis protocols](https://activemq.apache.org/components/artemis/documentation/latest/protocols-interoperability.html)
- [Apache Pulsar documentation](https://pulsar.apache.org/docs/next/concepts-messaging/) and [Redpanda documentation](https://docs.redpanda.com/current/)
- [gRPC core concepts](https://grpc.io/docs/what-is-grpc/core-concepts/) and [gRPC flow control](https://grpc.io/docs/guides/flow-control/)
- [HTTP Semantics, RFC 9110](https://www.rfc-editor.org/rfc/rfc9110) and [TLS 1.3, RFC 8446](https://www.rfc-editor.org/rfc/rfc8446)
- [OpenTelemetry Protocol](https://opentelemetry.io/docs/specs/otlp/) and [OpenTelemetry metrics data model](https://opentelemetry.io/docs/specs/otel/metrics/data-model/)
- [ClickHouse HTTP interface](https://clickhouse.com/docs/interfaces/http), [native protocol](https://clickhouse.com/docs/interfaces/tcp), and [deduplicating inserts on retries](https://clickhouse.com/docs/guides/developer/deduplicating-inserts-on-retries)
- [InfluxDB Line Protocol](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/)
- [QuestDB ILP ingestion](https://questdb.com/docs/ingestion/) and [QuestDB WAL](https://questdb.com/docs/concept/write-ahead-log/)
- [TDengine connectors](https://docs.tdengine.com/tdengine-reference/client-libraries/)
- [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html)
- [CloudEvents specification](https://github.com/cloudevents/spec)