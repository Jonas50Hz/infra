# Gateway Dashboard Provisioner

`gateway-dashboard-provisioner` is a root-owned persistent consumer of the
compacted `Masterdata` topic. It derives the active C37.118 source set from
canonical raw-Protobuf `SourceMasterdata` records and writes Grafana JSON into
the root `gateway-dashboard-data` volume.

On each start, the service manually assigns every `Masterdata` partition,
replays it from the beginning through captured end offsets, writes one complete
dashboard snapshot, then tails later records. It has no consumer group and
commits no offsets. This lets a recreated dashboard volume recover from Kafka's
compacted runtime state.

Grafana reads the volume through its dedicated **WAMA Gateways** file provider:

- **WAMA Gateway Fleet** is always present, including when no active sources
  exist.
- Each active source receives one deterministic dashboard with catalog
  provenance, endpoint metadata, unit-safe Druid trends, freshness, and latest
  records. Trends include every finite measurement; the latest-record table
  retains `quality_valid` so conservative synchronization uncertainty remains
  visible instead of suppressing the source data.
- A source-keyed Masterdata tombstone removes only that generated source page
  and its fleet link. It does not delete Druid history or the static PMU fixture
  dashboard.

The provisioner validates C37.118 endpoint identity, catalog provenance, signal
uniqueness, and the V1 `voltage`/`current`/`frequency`/`rocof` double-value
semantics before it writes files. Invalid input leaves the last complete snapshot
in place and marks the service unhealthy.

This service does not deploy, restart, or evaluate a gateway. The Forgejo C37
C37.118 gateway workflow remains the authority that publishes Masterdata and manages
only its approved adapter scope. Druid remains the live-value datasource; Trino
is not used to determine gateway membership or render these pages.

Validate the focused implementation with:

```sh
docker build --target test --file services/gateway-dashboard-provisioner/Dockerfile .
docker compose config --quiet
```