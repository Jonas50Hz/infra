# Measurement Session Contract-to-Download Test

`measurement-session-e2e` is a profile-gated one-shot test service. It has no
S3 or PostgreSQL credentials. The accompanying root script starts the catalog
API and browser, runs the static exporter with a unique session ID, then runs
this service.

The test independently decodes the raw-Protobuf Kafka record, waits for the
catalog through the browser's same-origin API path, verifies the download bytes
against the exporter fixture, verifies every declared CSV measurement from start
through end plus the source/date attachment name, and rejects redirects,
mutations, and leaked object-store details.

Run the complete flow with:

```sh
scripts/test-measurement-session-flow.sh
```