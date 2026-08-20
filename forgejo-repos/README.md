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

`forgejo-init` seeds each repository only when its remote has no refs; an
existing nonempty private repository is left unchanged. Each workflow deploys
only its one processor into its own marker-owned deployment root. A processor
repository may contain a gateway only for an explicit gateway-deployment test.
The parent `infra` repository retains all other assets, including the current
`pmu-gateway` and every infrastructure service.

No seed currently produces `Export` records. When an export-producing processor
is created, it must copy the canonical
[`../docs/wama/schema/iec104_export.proto`](../docs/wama/schema/iec104_export.proto)
into that new processor repository deliberately; it must not take ownership of
the root-owned IEC 104 exporter or receiver.