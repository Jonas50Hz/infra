# Trino

`trino` is the root-owned, single-node SQL federation coordinator for this
trusted local PoC. It exposes its HTTP query API and web UI on port 8085 and
provides read-only catalogs for Druid live measurements, the PostgreSQL Blobmeta
projection, and registered measurement-session artifacts:

- `druid.druid.live_measurements` connects to Druid's internal Broker through
  Avatica on `druid:8082`.
- `blobmeta.blobmeta_catalog` connects with the dedicated
  `trino_blobmeta_reader` PostgreSQL login created by `trino-init`.
- `sessions.wama.measurement_values` uses the dedicated
  `trino_session_reader` PostgreSQL login and SeaweedFS S3 storage through an
  Iceberg JDBC catalog.

Trino's built-in system read-only access control and the PostgreSQL reader role
prevent writes. It intentionally has no authentication or TLS because this is
a trusted-LAN PoC service. Do not expose it outside that boundary.

The internal-only `trino-session-writer` coordinator is responsible for adding
verified immutable session files to Iceberg. This reader never writes Iceberg
metadata or artifacts.