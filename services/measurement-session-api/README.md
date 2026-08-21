# Measurement Session API

`measurement-session-api` is the root-owned, local-PoC request boundary for
immutable `MeasurementSession` exports. Grafana opens its confirmation page
with a selected time range and MRIDs; the page submits one validated raw-Protobuf
request to Kafka.

The service owns request construction and publication only. It has no Druid,
SeaweedFS, PostgreSQL, Trino-writer, or Blobmeta credentials. The existing
`measurement-session-processor` remains the only component that reads source
values and materializes session artifacts.

The host mapping is loopback-only at `http://localhost:3004`. The UI generates
one canonical session UUID per open confirmation page, so a retry publishes the
same request identity. The processor's immutable receipt then makes Kafka
redelivery replay-safe. The completed-session link derives from
`GRAFANA_ROOT_URL`, which must be the browser-reachable Grafana origin (for
example, `http://<host-ip>:3001/`); use `GRAFANA_SESSION_DASHBOARD_URL` only to
override the exact destination. This trusted local PoC does not yet identify
individual Grafana users; production authentication and audit identity remain a
separate design increment.