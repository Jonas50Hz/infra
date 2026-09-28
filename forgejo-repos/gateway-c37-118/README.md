# C37.118 v2 gateway

This standalone Forgejo repository owns the reviewed C37.118.2-2011 (wire
version 2) catalog, its compacted raw-Protobuf `Masterdata` projection, and
only catalog-derived `c37-118-gateway-<source-id>` adapters. It is the explicit
Forgejo gateway-deployment PoC; it is not root infrastructure.

## Reviewed demonstration catalog

`catalog/sources/` is the reviewed, catalog-driven gateway set. It must contain
at least one strict YAML enrollment, but it has no fixed source count. The
checked-in sample contains five sources matching the manually operated
`c37-118-simulator` endpoints at `172.30.0.10`, ports `4712` through `4716`,
and PMU IDCODEs `1001` through `1005`. Each sample source has six phasor
magnitude channels (`VL1` through `IL3`), `FREQ`, and `DFREQ`: eight stable
MRIDs per source. The frequency MRIDs match the direct IEC 104 processor's
reviewed map.

The loader rejects unknown YAML fields, host names, non-v2 connections,
nonliteral IP addresses, invalid ranges, duplicate source/channel/selector/MRID
identities, and invalid quantity/unit/selector combinations. Do not rename a
source file or change an existing MRID. MRID migration is intentionally not a
catalog edit.

## Add or remove a gateway

To add an adapter, add a reviewed
`catalog/sources/<source-id>.yaml` file whose filename and `source_id` match,
with unique endpoint, PMU IDCODE, and MRID mappings. A trusted `main` deployment
publishes its Masterdata record and starts exactly one
`c37-118-gateway-<source-id>` adapter.

To remove an adapter, delete that source YAML file and deploy the reviewed
change. The publisher emits a tombstone only for the previously catalog-owned
source, and the guarded deployment removes only its matching recorded adapter.
Restoring the same reviewed YAML restores that source's Masterdata record and
only its matching adapter.

The local test suite and live verifier derive their expected source and MRID
sets from the current catalog; neither requires the five checked-in samples.

## Runtime

`masterdata-publisher` is one-shot. It validates the catalog, reads compacted
`Masterdata` through current end offsets, rejects malformed foreign ownership or
MRID collisions, publishes deterministic raw-Protobuf values ordered by
`source_id`, and emits tombstones only for source IDs previously published by
this catalog.

Each adapter asks its source for CFG-2 before requesting data. It derives the
fixed-width channel offsets and scales from CFG-2, accepts only V2/CRC-valid
frames, normalizes phasor magnitude, frequency, and ROCOF as raw-Protobuf
`MCCSMeasurementValue`, and waits for Kafka `acks=all`. It reconnects after
normal TCP/frame/mapping failures. A source timestamp may lead receipt by up to
250 ms at a reporting boundary; a larger future lead is rejected.
`quality.valid` is true only for a clean V2 `STAT`; `quality.substituted` maps
V2 post-processing.

The application-local `compose.yaml` declares only the one-shot publisher and
attaches only to the pre-existing external `wama-infra` network. The deployment
guard creates `generated-adapters.compose.yaml` after publication. It can start
or remove only `c37-118-gateway-<source-id>` names recorded in its own marker
root; it cannot operate root Compose, `pmu-gateway`, or the simulator.

## Validation and focused live proof

Repository-local tests:

```sh
docker build --target test -f Dockerfile .
docker compose -f compose.yaml config --quiet
```

With root infrastructure and a separately operated V2 source for every current
catalog entry running, deploy an approved `main` revision through Forgejo, then
run the complete proof from the marker-owned deployment root only:

```sh
WAMA_GATEWAY_C37_118_DEPLOY_ROOT=/var/lib/wama-gateway-c37-118 \
  scripts/verify_gateway_e2e.sh
```

The command reruns the idempotent publisher, proves the compacted Masterdata
projection exactly matches the current catalog, checks that the generated
adapter service set matches the catalog and is running, then uses a unique
non-committing consumer group to prove every current catalog MRID arrived with
matching keys, raw-Protobuf double values, explicit quality, and ordered
field/gateway/MCCS timestamps. It does not start, stop, or control the
simulator.
