# Measurement Session Exporter

`measurement-session-exporter` is an explicitly invoked one-shot service. It
loads one static finalized-session fixture, writes immutable artifacts and a
canonical manifest to the `wama-measurement-sessions` SeaweedFS bucket, then
publishes one raw-Protobuf `MeasurementSession` record to Kafka.

It does not consume `LiveMeasurement`, infer time windows, emit lifecycle
updates, or write PostgreSQL. Replaying the same fixture is idempotent only
when all existing object bytes and digests match.

Run it after the infrastructure readiness gate succeeds:

```sh
docker compose --profile measurement-session-export run --rm measurement-session-exporter
```

Set `MEASUREMENT_SESSION_ID_OVERRIDE` to a canonical UUID when an isolated test
run needs a distinct finalized session.

The default waveform fixture contains all 120 declared, ordered measurements
from the session start through its end. The exporter test suite rejects a fixture
whose CSV row count, timestamps, or measurement fields do not match the final
session metadata. The runtime exporter applies the same validation before it
writes any immutable object or Kafka record.