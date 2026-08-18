# WAMA Processors

This is the tracked `wama-processors` repository seed. It is intentionally
separate from the parent infrastructure checkout. `forgejo-init` creates the
private Forgejo repository and pushes its initial `main` commit automatically
when the remote has no refs. It never overwrites an existing nonempty private
repository.

After infrastructure bootstrap, clone the Forgejo repository to work on it:

```sh
git clone "https://<forgejo-host>/<owner>/wama-processors.git"
cd wama-processors
```

Do not add a Forgejo remote to the parent `infra` repository.

## Runtime boundary

This repository deploys only internal `processor-*` services. A
gateway may be added only for an explicitly declared Forgejo gateway-deployment
test. That exception never includes the current `pmu-gateway`, which remains in
the parent infrastructure repository. Its Compose project joins the
pre-existing external `wama-infra` Docker network, where the infrastructure
repository provides `kafka:9092`. Processors files never include, copy,
modify, or redeploy the infrastructure Compose project or its services. All
other assets remain owned and deployed from the parent infrastructure
repository.

## Processors

`processor-frequency-scale` is the checked-in service that converts fake PMU
frequency from Hz to millihertz as raw `MCCSMeasurementValue` Protobuf on
`LiveMeasurement`. It accepts only
`urn:wama:poc:pmu:bay-01:frequency` with its matching Kafka key, publishes
`urn:wama:poc:pmu:bay-01:frequency-millihertz`, preserves Common Format and
Kafka timestamps, and rejects its own derived records.

`processor-apparent-power` pairs the latest valid fake PMU voltage and current
for each phase and publishes `...:apparent-power-l1`, `...:apparent-power-l2`,
and `...:apparent-power-l3` on `LiveMeasurement`. It calculates apparent power
$S = U \times I$ in VA, preserves the triggering record's Common Format/Kafka
timestamp context, and rejects invalid, incomplete, non-source, and derived
records.

`templates/quixstreams-processor/` remains an inactive scaffold for a future
tracked processor:

```sh
python3 scripts/provision_processor.py frequency-scale
python3 scripts/validate_processors.py
```

Each processor owns manually editable Python/Quixstreams code, tests, a Compose
fragment, and optional YAML configuration. Processor transformations must be
deterministic for Kafka at-least-once replay, preserve required timestamp
context, and prevent feedback when both topics are `LiveMeasurement`. Stateful
processors must document their state lifetime and replay behavior.

### Provisioned processors
<!-- provisioned-processors:start -->
- [`processors/processor-apparent-power/`](processors/processor-apparent-power/) - `processor-apparent-power`
- [`processors/processor-frequency-scale/`](processors/processor-frequency-scale/) - `processor-frequency-scale`
<!-- provisioned-processors:end -->

The [`contracts/rtd_schema.proto`](contracts/rtd_schema.proto) file is a
deliberate processors-repository copy of the WAMA Common Format. Update it
intentionally when the infrastructure data contract changes; processors builds
do not import files from the parent checkout.

## Forgejo Actions

`.forgejo/workflows/processors.yaml` validates pull requests. A trusted `main`
push builds and publishes processor OCI images to Forgejo Packages, then deploys
only processors from the Forgejo checkout into
`WAMA_PROCESSORS_DEPLOY_ROOT`. The `wama-processors-ci` connection runs
validation and publishing; the `wama-processors-deploy` connection runs the
serialized deployment job. The workflow discovers every
`processors/processor-*` directory, so it tests, publishes, and deploys both
checked-in processors without a service-specific workflow entry. Bootstrap
supplies the trusted runner with a dedicated `write:package` credential for
private Forgejo image publication and pulls.

Any future gateway-deployment test must be explicitly declared and limited to
that test. It must not deploy, modify, or take ownership of the current
`pmu-gateway` or any root infrastructure service.

The infrastructure runner grants `wama-processors-deploy` access to the Docker
host for this local PoC. Limit write access to this Forgejo repository to
trusted users.