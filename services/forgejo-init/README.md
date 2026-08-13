# Forgejo bootstrap

`forgejo-init` runs after Forgejo becomes healthy. It creates the configured
administrator and private repository through the Forgejo API, creates the
deployment-root marker, then idempotently registers the `wama-ci` runner at
repository scope.

Configure the values in the root `.env`; a duplicate
[forgejo-init.env.example](forgejo-init.env.example) is available as a focused
reference. `FORGEJO_RUNNER_URL` must be reachable from the runner and its job
containers. For CI/CD, set it to the same HTTPS endpoint as `FORGEJO_ROOT_URL`.

Generated runner credentials and the temporary bootstrap API token are stored
only in the `forgejo-runner-data` volume. The bootstrap script preserves an
existing `WAMA_DEPLOY_ROOT` and requires its `.wama-deploy-root` marker before
the deployment helper will copy any checkout there.