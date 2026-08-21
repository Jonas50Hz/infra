# C37.118 Masterdata and Gateway Onboarding

## Status and Scope

This increment establishes reviewed C37.118 PMU masterdata, its runtime Kafka
projection, and a guarded legacy C37.118.2-2011 wire-version-2 TCP adapter for
each active catalog source. It does not alter the root-owned `pmu-gateway` or
simulator, run the root Compose project, mirror Masterdata to PostgreSQL, or
support version 3, CFG-3, UDP, TLS, source discovery, credentials, or raw-frame
persistence.

[`../../forgejo-repos/gateway-c37-118-onboarding/`](../../forgejo-repos/gateway-c37-118-onboarding/)
is the one explicitly declared gateway-deployment-test repository. It owns a
one-shot `masterdata-publisher` plus only source-scoped adapters rendered by its
marker-owned deployment guard.

## Authority and Runtime State

Git is the reviewed authority. A Power User changes a source YAML file on a
feature branch. Repository validation checks the catalog, raw-Protobuf contract,
v2 decoder, and deployment guard. A Systemexperte approves the change by
merging it to `main`, which builds one revision-labelled image and runs it from
its marker-owned deployment root.

Kafka's compacted `Masterdata` topic is the runtime projection. Git history
holds the proposal and approval trail. Each published record also contains a
catalog ID, Forgejo revision, and publication time. PostgreSQL remains limited
to the root-owned Blobmeta projection in this phase.

## Wire Contract

The canonical [`schema/masterdata.proto`](schema/masterdata.proto) defines
`wama.masterdata.v1.SourceMasterdata`.

| Item | V1 rule |
|---|---|
| Kafka key | Exact UTF-8 `source_id` |
| Non-null value | Deterministic raw-Protobuf `SourceMasterdata` |
| Decommission | Null value with the source key |
| Location | Required stable `site_id` and human-readable `display_name` |
| Endpoint | Required literal IPv4/IPv6 address and TCP port 1-65535 |
| PMU identity | Required C37.118 PMU IDCODE 1-65535 |
| Wire version | Required legacy C37.118 version 2 |
| Signal identity | Stable `signal_id`, explicit v2 selector, channel alias, and MRID |
| Signal semantics | Common Format scalar kind, quantity, and engineering unit |

V1 accepts C37.118 TCP PMUs only. Signals are `double_value` mappings for
voltage in `V`, current in `A`, frequency in `Hz`, and ROCOF in `Hz/s`. A
voltage/current signal names a fixed-width CFG-2 phasor magnitude channel;
frequency and ROCOF use their required singleton v2 values. Hostnames, default
ports, device credentials, capability discovery, non-PMU source protocols, and
secret distribution are intentionally excluded.

## Catalog Rules

The publisher rejects unknown YAML keys, invalid endpoint literals and port
ranges, non-v2 wire versions, duplicate source IDs, channels, selectors, or
MRIDs, and invalid selector/quantity/unit/value-kind combinations. Source
filenames match their stable source ID.

MRIDs are immutable after publication. A known `(source_id, signal_id)` cannot
change its MRID, and an active MRID cannot move between sources or signal IDs.
Location, endpoint, PMU IDCODE, and metadata updates are allowed. New signals
are allowed. A deliberate MRID migration needs a future explicit contract and
migration process rather than a silent catalog edit.

Before publishing, the publisher consumes `Masterdata` through current end
offsets. It rejects malformed existing records, keys owned by another catalog,
and MRID ownership collisions. It publishes active sources in deterministic
source-ID order and tombstones only source IDs previously owned by its catalog
but absent from the approved catalog.

## Deployment Boundary

The repository has the dedicated root
`/var/lib/wama-gateway-c37-118-onboarding`, marked with
`.wama-forgejo-gateway-onboarding-root`. Its guard copies only Git-tracked
files, rejects unsafe roots, infrastructure checkouts, extra Compose services,
and non-external networks, then checks the pulled image's OCI revision.

The publisher invocation remains one-shot:

```sh
docker compose run --rm --no-deps masterdata-publisher
```

After successful Masterdata reconciliation, the guard derives active source IDs
only from safe catalog filenames and writes a generated Compose overlay. Each
`c37-118-gateway-<source-id>` service uses the revision-verified image, has no
host port, joins only the pre-existing external `wama-infra` network, and runs
the persistent v2 adapter. The guard records only those generated service names
and a tombstone can stop/remove only a prior recorded name with matching Compose
labels. It does not create Kafka, Forgejo, Druid, or any root service; it does
not run the root `docker-compose.yml`; and it cannot manage `pmu-gateway`.

## Adapter Behavior

The adapter requests CFG-2 before accepting Data, derives all value offsets and
scaling from that configuration, and reconnects with bounded backoff after a
TCP, Kafka, CRC, size, version, or mapping failure. It emits phasor magnitudes,
absolute frequency in Hz, and ROCOF in Hz/s as raw-Protobuf
`MCCSMeasurementValue` values keyed by MRID. It sets `timestamp_field` from
`SOC` and `FRACSEC/TIME_BASE`; receipt time becomes both gateway and MCCS time.
Future field timestamps are rejected. Generic Common Format quality is limited
to `valid` and `substituted` from conservative v2 `STAT` handling; a separate
status-evidence contract remains necessary for LFR audit use.