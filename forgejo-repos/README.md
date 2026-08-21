# Forgejo Repository Seeds

This directory contains tracked seed content for repositories that may be
initialized and pushed to Forgejo. Each child directory is a separate
repository boundary. The parent `infra` repository is infrastructure-only and
must never be added as a Forgejo remote or pushed to Forgejo. Forgejo is
reserved for internal processor deployment and a deliberately declared
gateway-deployment test, not for any other infrastructure or repository asset.

Each active processor has its own seed and private Forgejo repository:

- [`processor-frequency-scale/`](processor-frequency-scale/) owns only
	`processor-frequency-scale`.
- [`processor-apparent-power/`](processor-apparent-power/) owns only
	`processor-apparent-power`.
- [`processor-frequency-iec104-export/`](processor-frequency-iec104-export/)
  owns only `processor-frequency-iec104-export`.
- [`processor-lfr-frequency-provision/`](processor-lfr-frequency-provision/)
	owns only `processor-lfr-frequency-provision`.

The explicitly declared C37.118 gateway-deployment-test seed is
[`gateway-c37-118-onboarding/`](gateway-c37-118-onboarding/). It owns only the
one-shot `masterdata-publisher` and guarded generated legacy-v2 adapters for
active approved sources in this increment.

`forgejo-init` seeds each repository only when its remote has no refs; an
existing nonempty private repository is left unchanged. Processor workflows
deploy only their one processor into their own marker-owned deployment root.
The onboarding workflow uses its separate marker-owned root to publish
Masterdata once and reconcile only its catalog-derived source adapters. The
parent `infra` repository retains all other assets, including the current
`pmu-gateway` and every infrastructure service.

`processor-frequency-iec104-export` deliberately copies the canonical
[`../docs/wama/schema/iec104_export.proto`](../docs/wama/schema/iec104_export.proto)
and writes direct configured PMU-frequency `M_ME_NC_1` requests to `Export`.
It is not the full LFR preferred-frequency algorithm. It must not take ownership
of the root-owned IEC 104 exporter, receiver, or browser.

`processor-lfr-frequency-provision` is the separate multi-PMU per-second LFR
core. It publishes a configured preferred-frequency Common Format value back to
`LiveMeasurement`; it does not yet create `Export` records or replace the
direct-export demonstration.