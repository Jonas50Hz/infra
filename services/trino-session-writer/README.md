# Trino Session Writer

`trino-session-writer` is an internal-only Trino 483 coordinator used by the
root-owned measurement-session query indexer. It has no host port and is the
only coordinator allowed to add verified canonical session Parquet artifacts to
the Iceberg metadata table.

The host-exposed `trino` service remains read-only for Grafana and direct SQL
queries. Both coordinators use PostgreSQL JDBC metadata and SeaweedFS S3 storage
for the same `sessions` Iceberg catalog.