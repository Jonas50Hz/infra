# C37.118 Gateway Onboarding

This standalone Forgejo repository is the explicitly scoped WAMA PoC
gateway-deployment-test boundary. It validates Git-authored legacy C37.118
version-2 PMU source records, reconciles their raw-Protobuf
`wama.masterdata.v1.SourceMasterdata` projections to compacted `Masterdata`,
and manages one isolated v2 TCP adapter per active source from its marker-owned
deployment root.

Each source YAML file declares a stable source ID, site ID, display name,
literal IP address, TCP port, C37.118 PMU IDCODE, `wire_version: 2`, and
explicit immutable signal-to-MRID mappings. A selector binds voltage/current to
a CFG-2 phasor magnitude channel and binds frequency/ROCOF to their singleton
v2 values. V1 accepts only the `voltage`, `current`, `frequency`, and `rocof`
double-value mappings with units `V`, `A`, `Hz`, and `Hz/s`.

The manually operated `~/c37-118-simulator` repository remains a fixed
five-PMU V2 fixture at `172.30.0.10:4712` through `172.30.0.10:4716`. The
initial catalog contains one source YAML for each fixture PMU, but reviewed
source YAML files may be added or removed. Active Masterdata records, generated
adapters, and verification expectations are derived from the current catalog;
this repository does not control, deploy, restart, or stop the simulator.

Run the repository checks from this directory:

```sh
docker build --target test --file Dockerfile .
```

Pull requests validate only. A trusted merge to `main` builds one image with
publisher and gateway entry points, runs `masterdata-publisher` once from the
dedicated `/var/lib/wama-gateway-c37-118-onboarding` deployment root, then
reconciles only generated `c37-118-gateway-<source-id>` services on the
existing external `wama-infra` network. The adapter requests CFG-2, emits
raw-Protobuf `MCCSMeasurementValue` records to `LiveMeasurement`, and applies
the documented conservative generic quality mapping. The guard never invokes
the root infrastructure Compose project or controls root `pmu-gateway`.

Deleting a catalog source publishes a null-valued tombstone for its source key
and removes only that source's previously managed adapter. Raw STAT/time-quality
retention, version 3, CFG-3, UDP, TLS, source discovery, and the root simulator
remain outside this repository's gateway scope.

After adapter reconciliation, the deployment automatically verifies fresh
records for every approved catalog MRID. Run the same verifier manually only
for focused troubleshooting:

```sh
cd /var/lib/wama-gateway-c37-118-onboarding
docker compose run --rm --no-deps masterdata-publisher \
	python -m gateway_c37_118_onboarding.verify_live_measurements
```

The verifier consumes from `LiveMeasurement` at `latest` through a unique,
non-committing consumer group. It ignores unrelated topic traffic and succeeds
only after it observes every approved catalog MRID with a well-formed
raw-Protobuf record, matching key, double value, explicit quality flag, and
ordered timestamps. The V2 simulator deliberately reports synchronization
uncertainty by default. Its five-PMU onboarding profile deliberately overrides
the V2 STAT for PMU IDs `1001` and `1002`, so bays 01 and 02 have
`quality.valid=true`; bays 03 through 05 remain `quality.valid=false`. The
verifier proves source transport and record integrity, not a real
synchronized-PMU quality claim. Set
`WAMA_LIVE_MEASUREMENT_VERIFY_TIMEOUT_SECONDS` to adjust its bounded 30-second
default.