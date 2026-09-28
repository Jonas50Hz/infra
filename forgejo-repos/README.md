# Forgejo Repository Checkouts and Seeds

This directory contains source content for repositories that may be initialized
and pushed to Forgejo. Each child directory is a separate repository boundary.
The parent `infra` repository is infrastructure-only and must never be added as
a Forgejo remote or pushed to Forgejo. Forgejo is reserved for internal
processor deployment and a deliberately declared gateway-deployment test, not
for any other infrastructure or repository asset.

## Each repository stands on its own

Every directory here must build, test, and deploy using only files inside
itself. A repository must not read the parent checkout at build time, and must
not depend on a shared runtime package installed from elsewhere in `infra`.
Concretely, each repository owns its own `Dockerfile` whose build context is its
own directory, its own `contracts/` copy of any `.proto` it needs, its own
tests, and its own `.forgejo/` workflow.

Verify a repository in isolation with its own directory as the build context:

```sh
docker build --target test -f <repository>/Dockerfile <repository>
```

This duplicates some content between repositories, and that is the accepted
trade. Independence is worth more here than removing the duplication: a shared
runtime would couple every processor to one version and one release.

`processor-authoring/` in the parent checkout is a scaffolding template library
used to author a new repository. Nothing in this directory depends on it at
build or deploy time, and nothing here may start doing so.

## Tracked repositories

Processors:

- [`processor-frequency-scale/`](processor-frequency-scale/) owns only
	`processor-frequency-scale`.
- [`processor-apparent-power/`](processor-apparent-power/) owns only
	`processor-apparent-power`.
- [`processor-frequency-iec104-export/`](processor-frequency-iec104-export/)
  owns only `processor-frequency-iec104-export`.
- [`processor-alarm-threshold/`](processor-alarm-threshold/) owns only
  `processor-alarm-threshold`.
- `processor-frequency-measurement-session` owns only the standard processor
	that turns Frequency Capture Episodes from `LiveMeasurement` into bounded
	`MeasurementSession` requests.
- [`processor-lfr-frequency-provision/`](processor-lfr-frequency-provision/)
  owns only the LFR per-second preferred-frequency processor. It vendors its
  own `sdk/` copy so its workflow validates without the parent checkout.
- [`processor-weather-map/`](processor-weather-map/) owns only the Berlin
  Open-Meteo weather collector that publishes to `LiveMeasurement`.

Gateways:

- [`gateway-c37-118/`](gateway-c37-118/) is the explicitly declared C37.118
  gateway-deployment test. It owns only the one-shot `masterdata-publisher` and
  guarded generated legacy-v2 adapters for active approved sources.
- [`gateway-c37-118-onboarding/`](gateway-c37-118-onboarding/) owns only the
  five-source onboarding variant targeted by
  [`../scripts/test-masterdata-onboarding.sh`](../scripts/test-masterdata-onboarding.sh).

[`history/`](history/) is not a repository. It holds archived bundles of former
nested worktrees; see its README.

Seven of these repositories are bootstrapped by default; `FORGEJO_MANAGED_REPOSITORIES`
in [`../.env.example`](../.env.example) lists them. `gateway-c37-118-onboarding`
and `processor-lfr-frequency-provision` are tracked here but are deliberately
not in that default set: the first exists for
[`../scripts/test-masterdata-onboarding.sh`](../scripts/test-masterdata-onboarding.sh),
and the second is a reviewed seed awaiting the open LFR decisions recorded in
[`../docs/reference/lfr-frequency-provision.md`](../docs/reference/lfr-frequency-provision.md).
Both still build and test standalone.

The tracked seeds are development checkouts and bootstrap sources. When a
Forgejo remote has no refs, `forgejo-init` copies their working content while
omitting nested `.git` metadata. The C37.118 gateway credential installer
accepts the co-located gateway checkout and rejects the parent infrastructure
checkout.

`forgejo-init` seeds each repository only when its remote has no refs; that
initial `main` push starts the repository workflow without a duplicate manual
dispatch. An existing nonempty private repository is left unchanged, then its
`processor.yaml` or `gateway.yaml` workflow is dispatched at `main` after
runner configuration on every bootstrap invocation. Processor workflows deploy
only their one processor into their own marker-owned deployment root. The
C37.118 gateway workflow uses its separate marker-owned root to publish
Masterdata once and reconcile only its catalog-derived source adapters. The
parent `infra` repository retains all other assets, including the current
`pmu-gateway` and every infrastructure service.

`processor-frequency-iec104-export` deliberately copies the canonical
[`../docs/wama/schema/iec104_export.proto`](../docs/wama/schema/iec104_export.proto)
and writes direct reviewed gateway-frequency `M_ME_NC_1` requests to `Export`
through its processor-owned mapping file. It is not the full LFR
preferred-frequency algorithm. It must not take ownership of the root-owned IEC
104 exporter, receiver, or browser.
