# Forgejo Repository Checkouts and Seeds

This directory contains source content for repositories that may be initialized
and pushed to Forgejo. `processor-alarm-threshold/` and
`gateway-c37-118/` are co-located private Git worktrees; the
remaining entries are tracked bootstrap seeds. Each child directory is a
separate repository boundary. The parent `infra` repository is
infrastructure-only and must never be added as a Forgejo remote or pushed to
Forgejo. Forgejo is reserved for internal processor deployment and a
deliberately declared gateway-deployment test, not for any other infrastructure
or repository asset.

The tracked bootstrap processor seeds are:

- [`processor-frequency-scale/`](processor-frequency-scale/) owns only
	`processor-frequency-scale`.
- [`processor-apparent-power/`](processor-apparent-power/) owns only
	`processor-apparent-power`.
- [`processor-frequency-iec104-export/`](processor-frequency-iec104-export/)
  owns only `processor-frequency-iec104-export`.
- `processor-frequency-measurement-session` owns only the standard processor
	that turns Frequency Capture Episodes from `LiveMeasurement` into bounded
	`MeasurementSession` requests.

Its five-source, EE-editable policy and PoC timing limits are defined in the
[data-flow contract](../docs/reference/wama-data-flow-contracts.md). It does
not represent Alarm lifecycle.

The co-located private
[`processor-alarm-threshold/`](processor-alarm-threshold/) worktree owns only
`processor-alarm-threshold`. The explicitly declared C37.118
gateway-deployment-test checkout is
[`gateway-c37-118/`](gateway-c37-118/). It owns only the
one-shot `masterdata-publisher` and guarded generated legacy-v2 adapters for
active approved sources in this increment.

The two co-located worktrees are development checkouts and bootstrap sources.
When a Forgejo remote has no refs, `forgejo-init` copies their working content
while omitting nested `.git` metadata. The C37.118 gateway credential installer
accepts the co-located gateway checkout and rejects the parent infrastructure
checkout.

`forgejo-init` seeds each repository only when its remote has no refs; an
existing nonempty private repository is left unchanged. Processor workflows
deploy only their one processor into their own marker-owned deployment root.
The C37.118 gateway workflow uses its separate marker-owned root to publish
Masterdata once and reconcile only its catalog-derived source adapters. The
parent `infra` repository retains all other assets, including the current
`pmu-gateway` and every infrastructure service.

`processor-frequency-iec104-export` deliberately copies the canonical
[`../docs/wama/schema/iec104_export.proto`](../docs/wama/schema/iec104_export.proto)
and writes direct reviewed gateway-frequency `M_ME_NC_1` requests to `Export`
through its processor-owned mapping file. It is not the full LFR
preferred-frequency algorithm. It must not take ownership of the root-owned IEC
104 exporter, receiver, or browser.
