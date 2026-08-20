# Measurement Session Request-Flow Test

`measurement-session-e2e` is a profile-gated one-shot verifier for the
root-owned request processor. It finds a known queryable Druid row, publishes
one complete and one partial raw-Protobuf `MeasurementSessionRequest`, validates
their keyed `Blobmeta` outputs, waits for PostgreSQL metadata materialization,
and verifies SeaweedFS SHA-256 metadata plus Parquet row coverage.

It receives the trusted local PostgreSQL and S3 credentials only for this
integration test; the persistent `blobmeta-catalog` never receives S3 access.

Run the complete flow with:

```sh
scripts/test-measurement-session-flow.sh
```