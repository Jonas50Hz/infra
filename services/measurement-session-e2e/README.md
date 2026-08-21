# Measurement Session Request-Flow Test

`measurement-session-e2e` is a profile-gated one-shot verifier for the
root-owned request processor. It finds a known queryable Druid row, publishes
one complete and one partial raw-Protobuf `MeasurementSessionRequest`, validates
their keyed `Blobmeta` outputs, waits for PostgreSQL metadata materialization,
and verifies SeaweedFS SHA-256 metadata plus Parquet row coverage. It then
waits for the query-index ledger and verifies public Iceberg query rows preserve
each Blobmeta identity and measurement count. It also requires Grafana's
provisioned Trino datasource to return a selected-session time-series frame.

The E2E verifier uses the public read-only Trino coordinator for its Druid,
Blobmeta metadata, MRID coverage, query-index ledger, and Iceberg queries.
Direct SeaweedFS and Parquet reads remain only for byte-level immutable-artifact
integrity evidence.

It receives trusted local S3 credentials only for immutable-artifact integrity
verification; its database and live-measurement queries use Trino. The persistent
`blobmeta-catalog` never receives S3 access.

Run the complete flow with:

```sh
scripts/test-measurement-session-flow.sh
```