# Measurement Session Query Indexer

`measurement-session-query-indexer` consumes compacted raw-Protobuf `Blobmeta`
records and makes only validated `COMPLETE` and `PARTIAL` v2 session artifacts
queryable through Iceberg.

Before registration it verifies SeaweedFS metadata, content hash, byte length,
Parquet schema version, field IDs, types, row identities, counts, and MRID
coverage. It registers the exact `measurements.parquet` object URI through the
internal-only `trino-session-writer`; it never scans a session directory because
that also contains the Blobmeta receipt.

Its mutable `session_query_index.registrations` PostgreSQL ledger is separate
from immutable `blobmeta_catalog`. A Blobmeta-scoped advisory lock and an
Iceberg `$files` reconciliation make Kafka replay safe after a crash between an
Iceberg append and Kafka offset commit.

The indexer owns this mutable schema and grants the public Trino reader only
`USAGE` and `SELECT`. That lets the E2E query path reconcile registration
evidence through Trino without granting direct PostgreSQL access or write
privileges.

The default consumer starts at `earliest` and fails closed when it encounters a
pre-v2 Blobmeta artifact. Deploy this slice only after the documented clean
`docker compose down -v` migration. `latest` is available solely for isolated
fresh-artifact verification with a unique consumer group; it does not index
existing artifacts.

Validate a fresh complete and partial artifact through Kafka, the registration
ledger, and the public read-only Iceberg catalog with:

```sh
scripts/test-measurement-session-flow.sh
```