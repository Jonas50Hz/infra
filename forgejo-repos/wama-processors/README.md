# WAMA Applications

This is an application repository seed. It is intentionally separate from the
parent infrastructure checkout. Initialize Git and push only from this
directory:

```sh
git init -b main
git add .
git commit -m "Initialize WAMA applications"
git remote add forgejo "https://<forgejo-host>/<owner>/wama-applications.git"
git push --set-upstream forgejo main
```

Do not add a Forgejo remote to the parent `infra` repository.

## Runtime boundary

This repository deploys only `processor-*` services. Its Compose project joins
the pre-existing external `wama-infra` Docker network, where the infrastructure
repository provides `kafka:9092`. Application files never include, copy, modify,
or redeploy the infrastructure Compose project or its services.

## Processors

`templates/quixstreams-processor/` is an inactive scaffold. Create a tracked
application processor with:

```sh
python3 scripts/provision_processor.py frequency-scale
python3 scripts/validate_processors.py
```

Each processor owns manually editable Python/Quixstreams code, tests, a Compose
fragment, and optional YAML configuration. The default transform emits no
records. Copy or intentionally update Common Format timestamps in custom code,
make the transform idempotent, and prevent a processor from consuming its own
derived records when both topics are `LiveMeasurement`.

### Provisioned processors
<!-- provisioned-processors:start -->
<!-- provisioned-processors:end -->

The [`contracts/rtd_schema.proto`](contracts/rtd_schema.proto) file is a
deliberate application-repository copy of the WAMA Common Format. Update it
intentionally when the infrastructure data contract changes; application builds
do not import files from the parent checkout.

## Forgejo Actions

`.forgejo/workflows/applications.yaml` validates pull requests. A trusted
`main` push builds and publishes processor OCI images to Forgejo Packages, then
deploys only processors from the Forgejo checkout into
`WAMA_APPS_DEPLOY_ROOT`. The workflow succeeds without publishing or deploying
when no processor has been provisioned.

The infrastructure runner grants `wama-app-deploy` access to the Docker host
for this local PoC. Limit write access to this Forgejo repository to trusted
users.