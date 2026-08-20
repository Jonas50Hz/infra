# Forgejo API and Operations

Use the public endpoint for developer and automation clients. The current
Forgejo service is version 16.0.2 and exposes its versioned REST API under
`/api/v1`.

```sh
export FORGEJO_ROOT_URL='https://forgejo.example.test'
export FORGEJO_API_URL="${FORGEJO_ROOT_URL%/}/api/v1"
export FORGEJO_OWNER='wama-admin'
export FORGEJO_REPOSITORY='processor-frequency-scale'
export FORGEJO_API_TOKEN='<token kept outside Git>'
```

For a local stack, `FORGEJO_ROOT_URL` may be HTTP. For an externally reachable
setup, set `FORGEJO_DOMAIN`, `FORGEJO_ROOT_URL`, and `FORGEJO_RUNNER_URL`
consistently to the same HTTPS endpoint. The runner can use
`host.docker.internal` for checkout, but its package image host is deliberately
rewritten to `127.0.0.1:3000`; do not change that behavior in a workflow.

## Authentication

Use a personal API token for routine API calls:

```sh
curl --fail --silent --show-error \
  --header "Accept: application/json" \
  --header "Authorization: token $FORGEJO_API_TOKEN" \
  "$FORGEJO_API_URL/user"
```

Create a token in Forgejo through `Settings -> Applications`, limiting it to
what the API operation needs. The token is shown only once. Store it in a
credential manager or protected environment variable, never in the repository,
workflow YAML, `.env.example`, Git remote, shell history, or command output.

`forgejo-init` is the sole bootstrap exception: it uses the configured
administrator's Basic authentication inside the trusted Compose container to
create the initial private repository and the runner-managed package token.
Do not copy the administrator password to developer workstations.

## Repository Inspection and Bootstrap

First verify the server and repository:

```sh
curl --fail --silent --show-error "${FORGEJO_ROOT_URL%/}/api/healthz"
curl --fail --silent --show-error \
  --header "Accept: application/json" \
  --header "Authorization: token $FORGEJO_API_TOKEN" \
  "$FORGEJO_API_URL/repos/$FORGEJO_OWNER/$FORGEJO_REPOSITORY"
```

For a brand-new instance, the trusted bootstrap operation is equivalent for
each processor repository to:

```sh
curl --fail --silent --show-error \
  --user "$FORGEJO_BOOTSTRAP_ADMIN_USERNAME:$FORGEJO_BOOTSTRAP_ADMIN_PASSWORD" \
  --header "Content-Type: application/json" \
  --data '{"name":"processor-frequency-scale","private":true,"auto_init":false}' \
  "$FORGEJO_API_URL/user/repos"
```

Only make that request after a `GET /repos/<owner>/<processor-repository>`
returns not found. The resulting repository must report `"private": true`.
Then use `git ls-remote` to inspect refs. If any ref exists, stop: bootstrap
must leave that repository untouched. If it has no refs, the bootstrap service
creates the initial `main` commit and pushes it without `--force`. The PoC
bootstraps both `processor-frequency-scale` and `processor-apparent-power`.

## Git and Pull Requests

Clone the remote through HTTPS or the documented SSH endpoint. Feature branches
and pull requests are the preferred review route. Let Git use a credential
helper or SSH key; do not embed a token in the remote URL.

```sh
git clone "${FORGEJO_ROOT_URL%/}/${FORGEJO_OWNER}/${FORGEJO_REPOSITORY}.git"
cd "$FORGEJO_REPOSITORY"
git switch -c feature/<short-change-name>
python3 -m unittest discover -s tooling-tests -v
docker build --target test -f Dockerfile .
git add <changed-files>
git commit -m "Describe the processor change"
git push --set-upstream origin feature/<short-change-name>
```

Create a pull request with the UI or this API request after replacing the
placeholders with JSON-safe values:

```sh
curl --fail --silent --show-error --request POST \
  --header "Accept: application/json" \
  --header "Authorization: token $FORGEJO_API_TOKEN" \
  --header "Content-Type: application/json" \
  --data '{
    "title":"Describe the processor change",
    "head":"feature/<short-change-name>",
    "base":"main",
    "body":"Validation: python unit tests and processor validation passed."
  }' \
  "$FORGEJO_API_URL/repos/$FORGEJO_OWNER/$FORGEJO_REPOSITORY/pulls"
```

The pull request triggers validation. Merge after the expected Actions run
succeeds. A trusted maintainer may also make a locally validated fast-forward
push directly to `main`; it runs the same validation, publication, and
deployment sequence. Never force-push `main`. Manual Docker deployment from a
developer checkout is not the replacement path.

## Actions and Deployment

Each checked-in workflow is `.forgejo/workflows/processor.yaml`:

| Event or ref | Jobs that may run | Result |
| --- | --- | --- |
| Pull request | `validate` | Tests and processor validation only. |
| Push to `main` | `validate`, then `publish`, then `deploy` | Builds, pushes, and deploys that processor. |
| Manual dispatch on a non-`main` ref | `validate` | Tests only. |
| Manual dispatch on `main` | `validate`, then `publish`, then `deploy` | Intentional redeployment of `main`. |

Each workflow's two repository-scoped runner connections are intentional. For
repository `<repository>`, the connections are:

- `wama-<repository>-ci` executes validation and package publication.
- `wama-<repository>-deploy` has that repository's host-visible deployment root
  and runs its serialized deployment job.

Inspect runner registration without changing it:

```sh
curl --fail --silent --show-error \
  --header "Accept: application/json" \
  --header "Authorization: token $FORGEJO_API_TOKEN" \
  "$FORGEJO_API_URL/repos/$FORGEJO_OWNER/$FORGEJO_REPOSITORY/actions/runners"
```

A manual workflow dispatch is a production-impacting operation in this PoC
when its `ref` is `main`. Dispatch it only when a known `main` revision should
be revalidated, published, and deployed:

```sh
curl --fail --silent --show-error --request POST \
  --header "Accept: application/json" \
  --header "Authorization: token $FORGEJO_API_TOKEN" \
  --header "Content-Type: application/json" \
  --data '{"ref":"main"}' \
  "$FORGEJO_API_URL/repos/$FORGEJO_OWNER/$FORGEJO_REPOSITORY/actions/workflows/processor.yaml/dispatches"
```

Follow the run in the Forgejo Actions UI. For a failed deployment, fix the
source or deployment precondition and create a new validated `main` revision
where possible. Do not force-push, hand-edit the managed deployment root, or
run the parent infrastructure Compose project from Forgejo.

## Deployment Safety Contract

Each repository-local `scripts/deploy_processor.py` enforces the parts that a
workflow must preserve:

- `WAMA_PROCESSOR_DEPLOY_ROOT` is absolute, not `/`, not a symlink, outside the
  Forgejo workspace, and contains `.wama-forgejo-processor-root`.
- Only Git-tracked files from that processor repository are synchronized to the managed
  deployment root; unmanaged files are not overwritten.
- The rendered project contains exactly its expected `processor-*` service and
  no other service names.
- Deployment uses the existing external `wama-infra` network, pulls `main`, and
  verifies every running image's `org.opencontainers.image.revision` matches the
  triggering commit.

Any proposal that violates this contract belongs in the parent infrastructure
repository, not this Forgejo workflow.