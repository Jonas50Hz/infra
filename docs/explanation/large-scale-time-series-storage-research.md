(exp_large_scale_time_series_storage_research)=

```{meta}
:description: Research framing the database trade-offs for sustained high-rate WAMA Common Format storage and long retention.
```

# Large-scale time-series storage research

This Explanation frames a decision about the canonical store for sustained,
long-retained WAMA measurements. It does not select a product or prescribe an
implementation. The current PoC uses Druid for raw-Protobuf `LiveMeasurement`
records, while VictoriaMetrics remains the store for infrastructure telemetry;
those established roles are described in the
[WAMA architecture](wama-architecture.md).

The researchers had no network access, so primary-source URLs are supplied but
not live-verified in this session. Verify the applicable product version and
edition before an ADR.

## Proposed workload, not demonstrated capacity

The following is a **proposed** decision workload. A record is one normalised
Common Format scalar, not a C37.118 frame.

| Measure | Proposed value |
|---|---|
| Sustained rate | 100,000 normalised scalar records per second |
| One day | 8.64 billion records |
| 30-day month | 259.2 billion records |
| 365-day year | 3.1536 trillion records |
| Six-week audit interval | 362.88 billion records |
| Signal shape | 250 PMUs, each with 8 MRIDs at 50 Hz, or $250 \times 8 \times 50 = 100{,}000$ records per second |

Known PoC fixtures establish context, not a storage benchmark. The
[five-PMU C37.118 gateway fixture](../reference/c37-118-masterdata-gateway.md)
produces approximately 2,000 normalised records per second. The manually armed
[one-hundred-PMU V2 profile](../../../c37-118-simulator/profiles/one-hundred-pmu-v2.yaml)
corresponds to a derived post-adapter workload shape of 40,000 normalised rows
per second ($100 * 8 * 50$); this is not raw simulator-emitted Common
Format/Kafka throughput. Neither result demonstrates 100,000 records per second
through the WAMA data path. A synthetic normalised-record workload is therefore
necessary for the proposed rate.

## What the store must preserve

The canonical input is raw-Protobuf `MCCSMeasurementValue`, defined by the
[Common Format schema](../wama/schema/rtd_schema.proto) and its
[data-flow contract](../reference/wama-data-flow-contracts.md). A storage
choice needs to retain the following properties rather than flatten away the
semantics that downstream processors use:

- MRID identity; field, gateway, and MCCS timestamps; and a defined canonical
  query timestamp.
- The typed scalar `oneof`, including the distinction between numeric, Boolean,
  string, and timestamp values.
- Optional quality flags in a form that preserves absent versus `false`.
- Event and replay identity. The current schema has no event ID, so an identity
  and deduplication/replay contract must be designed rather than assumed.
- Interval scans, live latest-value and trend queries, and bounded extraction
  for a `MeasurementSession`.

Raw frames and waveform data do not belong in this record store or on Kafka;
they remain in SeaweedFS or compatible object storage. The current
[`MeasurementSession` worker limit](../../services/measurement-session-processor/src/measurement_session_processor/config.py)
is five million rows, which makes ordered extraction a material workload rather
than a convenience query. Six-week LFR audit evidence remains an unresolved
policy and contract question, not a retention number implied by the current
PoC.

## Candidate directions

The comparison concerns architecture fit and risks, not a claim that any
product can universally sustain the proposed rate. "Core" below means the
project's documented base offering; Enterprise, cloud, managed, and support
terms need separate version and procurement review.

| Candidate | Architecture fit | Major risk or constraint | Verdict |
|---|---|---|---|
| ClickHouse | Columnar analytical storage with Kafka ingestion, MergeTree layout, TTL, replication, and Protobuf-format documentation makes it a natural fresh candidate for a long-lived typed record projection. | Partition/order keys, retry semantics, deduplication, compaction, replica cost, and operational ownership must be designed; a Kafka engine is not by itself an end-to-end delivery guarantee. Core self-management and managed-service operation are distinct choices. | Strongest fresh candidate for long-retained, raw queryable records. |
| Apache Druid | Existing WAMA continuity: direct Kafka raw-Protobuf ingestion, no-rollup records, and segment-oriented historical query are already represented by the PoC. | The present single-server PoC is not production-shaped. Cluster services, segment lifecycle, deep storage, compaction, retention, and replay need a production evaluation. | Production-shaped Druid is the continuity candidate. |
| Apache Pinot | Stream ingestion and real-time analytical serving fit a specialised low-latency serving tier. | Long retention, storage cost, and operational complexity are not justified merely by ingest rate. Open-source core and any managed distribution still require the same WAMA identity and typed-value model. | Justified only by a separately measured sub-second, high-concurrency serving need. |
| TimescaleDB | PostgreSQL SQL, hypertables, retention, and batched `COPY` provide a familiar relational route for a typed record projection. | It is conditional: batched durable writes, retention/compaction behaviour, and high availability must be proven for this workload. Feature, support, and managed-service choices are edition and deployment specific. | Conditional candidate, not a default. |
| InfluxDB 3 | Its Core write, SQL, and administration material makes it worth considering where its time-series model and operations fit procurement constraints. | The required long-retention, recovery, and operational posture must be evaluated against the exact Core, Enterprise, or managed edition; the WAMA typed-value and replay model still needs explicit mapping. | Optional constrained challenger as InfluxDB 3 Enterprise or managed. |
| QuestDB | SQL querying, ingestion, WAL, and Kafka-connector documentation make it a compact challenger for a record-oriented time-series projection. | Durability, recovery, high availability, retention operations, and support must be demonstrated for the selected open-source or Enterprise posture. | Optional constrained challenger as QuestDB Enterprise. |
| VictoriaMetrics | It remains appropriate for the infrastructure telemetry role already assigned to it in WAMA. | A label-heavy metrics model distorts typed Common Format values, quality presence, and event/replay identity. Community, cluster, and Enterprise variants do not change that boundary. | Do not use as canonical measurement storage. |

## A likely comparison shortlist

This is a prioritised comparison set, not a final recommendation. ClickHouse is
the strongest fresh candidate for long-retained raw queryable records. A
production-shaped Druid deployment is the continuity candidate because the
PoC already decodes the authoritative Protobuf contract into a no-rollup
datasource. InfluxDB 3 Enterprise or managed, and QuestDB Enterprise, are
optional constrained challengers where operations or procurement materially
favour them.

TimescaleDB remains conditional on evidence of batched durable writes and high
availability. Pinot should enter the shortlist only when a separate test
establishes a sub-second, high-concurrency serving need; it should not be
selected merely because the intake rate is high.

## Projection shape, not a metrics relabelling

One useful comparison object is a flattened typed record table. Its physical
layout will vary by candidate, but its logical fields should remain recognisable:

| Field group | Logical fields and purpose |
|---|---|
| Identity | `mrid`, source or gateway identity, and a designed stable `event_identity`; optionally retain Kafka topic, partition, and offset as ingest trace rather than treating them as the event ID. |
| Time | `timestamp_mccs` as canonical timestamp, plus nullable `timestamp_field` and `timestamp_gateway`. |
| Typed value | `value_kind` plus nullable `double_value`, `int_value`, `uint_value`, `bool_value`, `string_value`, and `timestamp_value`. Exactly one typed value is populated for a valid Common Format record. |
| Quality | `quality_present`, then a presence/value pair for every known flag, such as `quality_valid_present` and `quality_valid`. This preserves absent versus `false` and leaves room for future flags. |
| Replay and provenance | Producer/source version, ingestion time, schema version, and any idempotency or replay marker defined by the future event-identity contract. |

Do not model each sample as a label-heavy infrastructure metric. Such a model
would make cardinality and identity implementation details while losing the
typed scalar and optional-quality contract. Raw frame and waveform object
identity belongs alongside object storage metadata, not as a large value in the
measurement record table.

## Vendor-neutral evaluation gate

Before an ADR, each serious candidate should be judged against the same
measurable workload. This makes a result about the WAMA contract rather than a
vendor demonstration.

| Test dimension | Evidence to collect |
|---|---|
| Sustained ingestion | 24 hours at 100,000 normalised scalar records per second, followed by a 15-minute 125,000-record-per-second burst. |
| Cardinality | Representative runs at 2,000, 20,000, and 100,000 MRIDs. |
| Delivery behaviour | Out-of-order arrivals, retry, replay, and duplicate handling, with completeness and replay behaviour recorded. |
| Representative reads | Interval/latest/trend queries approximating 8 MRIDs over 5 minutes, 1 hour, and 24 hours. |
| Session path | The permitted five-million-row ordered `MeasurementSession` extraction. |
| Resilience | Failure and recovery during ingestion and query, including the post-recovery replay outcome. |
| Retention shape | A retained and compacted 30-day-equivalent corpus, evaluated without replacing it with an unrepresentative aggregate-only dataset. |
| Resources | Ingest lag; visibility p95/p99; query latency; completeness/replay behaviour; bytes per row including replicas, WAL, compaction, and backups; and CPU, RAM, I/O, and network use. |

No numerical acceptance targets beyond the proposed workload are set here in
the absence of a product SLO. The gate is intended to expose the trade-offs
that an ADR must make explicit, especially the cost of retention, recovery, and
semantically correct replay.

## Primary-source reading

These official pages are reading material, not live-verified evidence from this
research session.

- **ClickHouse:** [Kafka engine](https://clickhouse.com/docs/engines/table-engines/integrations/kafka), [MergeTree](https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree), [TTL](https://clickhouse.com/docs/guides/developer/ttl), [replication](https://clickhouse.com/docs/engines/table-engines/mergetree-family/replication), [Protobuf](https://clickhouse.com/docs/interfaces/formats/Protobuf)
- **Apache Druid:** [Kafka ingestion](https://druid.apache.org/docs/latest/ingestion/kafka-ingestion/), [Protobuf](https://druid.apache.org/docs/latest/development/extensions-core/protobuf/), [segments](https://druid.apache.org/docs/latest/design/segments/), [cluster configuration](https://druid.apache.org/docs/latest/operations/cluster-configuration/)
- **Apache Pinot:** [stream ingestion](https://docs.pinot.apache.org/basics/data-import/pinot-stream-ingestion), [components](https://docs.pinot.apache.org/basics/concepts/components), [table configuration](https://docs.pinot.apache.org/configuration-reference/table)
- **TimescaleDB and PostgreSQL:** [hypertables](https://docs.timescale.com/use-timescale/latest/hypertables/about-hypertables/), [Hypercore](https://docs.timescale.com/use-timescale/latest/hypercore/), [retention](https://docs.timescale.com/use-timescale/latest/data-retention/), [`COPY`](https://www.postgresql.org/docs/current/sql-copy.html)
- **InfluxDB 3:** [Core write](https://docs.influxdata.com/influxdb3/core/write-data/), [SQL](https://docs.influxdata.com/influxdb3/core/query-data/sql/), [administration](https://docs.influxdata.com/influxdb3/core/admin/), [Enterprise](https://docs.influxdata.com/influxdb3/enterprise/)
- **QuestDB:** [ingestion](https://questdb.com/docs/ingestion/), [WAL](https://questdb.com/docs/concept/write-ahead-log/), [query](https://questdb.com/docs/query/), [Kafka connector](https://github.com/questdb/kafka-questdb-connector), [Enterprise](https://questdb.com/enterprise/)
- **VictoriaMetrics:** [data ingestion](https://docs.victoriametrics.com/victoriametrics/data-ingestion/), [key concepts](https://docs.victoriametrics.com/victoriametrics/keyconcepts/), [cluster](https://docs.victoriametrics.com/victoriametrics/cluster-victoriametrics/)