# Trino PostgreSQL Reader Bootstrap

`trino-init` is the root-owned, one-shot PostgreSQL privilege bootstrap for the
Trino services. It waits for `blobmeta-catalog` to own and initialize its
immutable schema, then idempotently creates the local-PoC
`trino_blobmeta_reader`, `trino_session_reader`, and `trino_session_writer`
logins.

The Blobmeta role receives only database connect, `blobmeta_catalog` schema
usage, and `SELECT` privileges on current and future catalog tables. It never
owns or modifies Blobmeta data. The root-owned `blobmeta-catalog` service
remains the only owner and writer of its schema.

The bootstrap also owns the two Trino 483 Iceberg JDBC metadata tables in
`iceberg_catalog`. The session reader can only select their metadata, while the
internal writer can append and update it. Neither role can create PostgreSQL
schemas or alter the immutable Blobmeta catalog.