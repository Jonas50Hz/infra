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

The checked-in `pmu-bay-01` record mirrors the eight MRIDs emitted by the
root-owned fake PMU fixture. It is catalog evidence only; this repository does
not control, deploy, or restart that fixture.

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