---
name: forgejo-processors-cicd
description: 'Use when working with the WAMA Forgejo REST API, private processor-frequency-scale or processor-apparent-power repositories, Git clone or push workflow, pull requests, Forgejo Actions CI/CD, processor image publication, deployment, or an explicitly scoped gateway deployment test. Keeps root infrastructure outside Forgejo.'
argument-hint: 'Describe the processor, gateway test, API, Git, or CI/CD task'
user-invocable: true
disable-model-invocation: false
---

# Forgejo Processors CI/CD

Use this skill for the separate Forgejo processor repositories. Each repository
owns one internal `processor-*` service and its isolated deployment root.

## Boundaries

- Forgejo owns internal `processor-*` services and their repository-local
  Compose project only.
- A gateway is eligible only when the request explicitly declares a
  Forgejo-based gateway-deployment test. The current `pmu-gateway` is always
  root infrastructure and is never a Forgejo deployment target.
- Never add a Forgejo remote to the parent `infra` repository. Never push,
  copy, modify, deploy, or run the root `docker-compose.yml` from this workflow.
- A processor project connects to the pre-existing external `wama-infra` Docker
   network. It does not create or own Kafka, Forgejo, or any other infrastructure
   service.

## Choose the Route

| Need | Correct route |
| --- | --- |
| Inspect or bootstrap a private repository | Use the REST API as described in [API and operations](./references/api-and-operations.md). Only `forgejo-init` may seed a repository with no refs. |
| Change a processor, test, contract copy, or workflow | Clone that processor's Forgejo repository, work on a feature branch, run local validation, push the branch, and open a pull request. |
| Publish and deploy a processor revision | Merge or push an already validated change to `main`. The checked-in workflow performs the rest. |
| Run a known `main` revision again | Manually dispatch `processors.yaml` only after confirming that deployment is intended; it also publishes and deploys. |
| Change root infrastructure or `pmu-gateway` | Stop here and work in the parent infrastructure repository, outside Forgejo. |

## Procedure

1. Establish the external Forgejo endpoint and repository identity. For a
   client, derive `FORGEJO_API_URL` from the public `FORGEJO_ROOT_URL`; the
   bootstrap container alone uses its internal `http://forgejo:3000/api/v1`
   endpoint. Confirm the server health and private repository before making a
   change. Use a personal API token only through an environment variable or a
   credential manager, never in a remote URL, commit, workflow file, or log.

2. For a new, empty Forgejo instance, let `forgejo-init` perform the idempotent
   API create-and-check operation for `processor-frequency-scale` and
   `processor-apparent-power`. It must create each as private with
   `auto_init: false`, inspect each remote's refs, seed only when there are no
   refs, and leave a nonempty repository unchanged. Do not replace this with a
   forced Git push or a manual reseed.

3. For normal development, clone the Forgejo repository rather than the seed or
   parent checkout. A feature branch and pull request are the preferred review
   route. Create a feature branch, run the repository checks, commit an
   intentional change, and push the branch:

   ```sh
   export FORGEJO_REPOSITORY=processor-frequency-scale
   git clone "${FORGEJO_ROOT_URL%/}/${FORGEJO_OWNER}/${FORGEJO_REPOSITORY}.git"
   cd "$FORGEJO_REPOSITORY"
   git switch -c feature/<short-change-name>
   python3 -m unittest discover -s tooling-tests -v
   docker build --target test -f Dockerfile .
   git add <changed-files>
   git commit -m "Describe the processor change"
   git push --set-upstream origin feature/<short-change-name>
   ```

   Use normal fast-forward Git operations. A trusted maintainer may instead
   push a locally validated fast-forward commit directly to `main`; that starts
   validation followed by publication and deployment. Do not force-push `main`,
   and do not put a personal access token in `origin`.

4. When using the review route, open a pull request against `main` through
   Forgejo's UI or REST API. The `pull_request` event runs validation only.
   Wait for it to test that repository's deployment guard and processor image
   before merging.

5. Treat a `main` push as a deployment request. On `main`, the workflow runs:
   `validate` on that repository's CI connection, `publish` on the same CI
   connection, and serialized `deploy` on that repository's deployment
   connection. Publishing tags its one image with both `sha-$FORGEJO_SHA` and
   `main`; deployment pulls `main` and verifies the OCI revision label matches
   `FORGEJO_SHA`.

6. Keep CI/CD changes constrained to the established control flow. Pin external
   actions to commit SHAs, preserve the `validate -> publish -> deploy`
   dependency chain, retain the `main` condition on publish and deploy, and
   retain the repository-specific deployment concurrency group with
   `cancel-in-progress: false`. Do not use `pull_request_target` or expose the
   package credential to untrusted code.

7. Use the runner-injected dedicated `write:package` credential only in trusted
   workflow jobs for Forgejo Packages. `FORGEJO_TOKEN` cannot publish the
   private package blobs in this PoC. Never replace the runner credential with
   a checked-in secret or a developer's personal package token.

8. If deployment fails, inspect the failed job before retrying. Confirm the
   deployment root remains marker-owned, outside the runner workspace, and its
   rendered Compose project has only `processor-*` services. Do not compensate
   by running the root infrastructure Compose project or modifying the root
   stack from a workflow.

## Completion Checks

- The API reports the intended private processor repository; an existing
   repository with refs was not reseeded.
- The change was locally validated and either reviewed through a pull request or
   fast-forward pushed to `main` by a trusted maintainer.
- The matching Actions run completed `validate`; a `main` run also completed
  `publish` and serialized `deploy` when processors exist.
- Deployed processor containers use images labelled with the triggering commit.
- No root infrastructure, unapproved gateway, credentials, or parent
  repository assets were included in the Forgejo change.

## Reference

Read [API and operations](./references/api-and-operations.md) for authenticated
`curl` templates, repository bootstrap rules, pull-request creation, runner
inspection, and intentional workflow dispatch.