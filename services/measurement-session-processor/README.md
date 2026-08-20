# Measurement Session Processor

`measurement-session-processor` is a root-owned Kafka worker. It consumes
raw-Protobuf `MeasurementSessionRequest` messages from `MeasurementSession`,
queries the requested historical rows from Druid, writes one immutable Parquet
artifact to `wama-measurement-sessions`, then emits an immutable raw-Protobuf
`Blobmeta` result keyed by its `blob_id`.

It keeps individual measurement rows in Druid and SeaweedFS. PostgreSQL receives
only the compacted `Blobmeta` metadata projection through `blobmeta-catalog`.
Valid requests that find no data for some MRIDs produce `PARTIAL`; bounded
validation failures produce an auditable `REJECTED` result. The worker stores a
Blobmeta receipt beside each result, so at-least-once Kafka delivery republishes
identical result bytes without re-querying Druid.

The default request limits are 32 MRIDs, 24 hours, 5,000,000 rows, and a 4 GiB
artifact. Override them with the `MEASUREMENT_SESSION_*` environment variables
in [compose.yaml](compose.yaml).

Scale the persistent worker pool up to the `MeasurementSession` topic partition
count:

```sh
docker compose up -d --scale measurement-session-processor=12
```