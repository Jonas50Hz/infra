# Trino Session Initializer

`trino-session-init` is the root-owned, one-shot initializer for the
measurement-session Iceberg table. It connects only to the internal
`trino-session-writer` coordinator and idempotently creates
`sessions.wama.measurement_values` with the v2 immutable session Parquet
schema.

It does not write measurement rows or change canonical session artifacts. The
public `trino` coordinator remains read-only.