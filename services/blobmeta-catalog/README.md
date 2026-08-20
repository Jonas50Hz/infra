# Blobmeta Catalog

`blobmeta-catalog` consumes raw-Protobuf `Blobmeta` records from the compacted
Kafka topic and creates the immutable `blobmeta_catalog` projection in
PostgreSQL. It stores session identifiers, timing, status, request/artifact
integrity fields, copied metadata, and per-MRID row coverage. It does not have
SeaweedFS credentials and never copies individual measurements into PostgreSQL.

Kafka remains the source of truth. The catalog transaction completes before the
consumer commits an offset; an at-least-once replay is accepted only when the
raw Blobmeta bytes have the same SHA-256 digest as the existing `blob_id` row.