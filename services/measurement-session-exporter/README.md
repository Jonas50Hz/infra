# Measurement Session Exporter

`measurement-session-exporter` is the local, read-only CSV download surface for
the selected **WAMA Measurement Sessions** Grafana view. It accepts one
canonical completed-session `blob_id`, the currently selected MRIDs and value
types, and the dashboard time range. It issues one fixed query through the
public read-only Trino coordinator and streams the matching chart values as a
CSV attachment.

The service is mapped only to loopback on port `3005` by default. It has no
Kafka, Druid, SeaweedFS, PostgreSQL, or Trino-writer access, and it cannot run
arbitrary SQL.

Run its focused tests with:

```sh
docker build --target test --file services/measurement-session-exporter/Dockerfile .
```