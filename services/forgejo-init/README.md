# Forgejo bootstrap

`forgejo-init` runs after Forgejo becomes healthy. It creates the configured
administrator and the empty private `wama-applications` repository through the
Forgejo API, creates the application deployment-root marker, then idempotently
registers the `wama-applications` runner at application-repository scope.

Configure the values in the root `.env`; a duplicate
[forgejo-init.env.example](forgejo-init.env.example) is available as a focused
reference. `FORGEJO_RUNNER_URL` must be reachable from the runner and its job
containers. For CI/CD, set it to the same HTTPS endpoint as `FORGEJO_ROOT_URL`.

Only runner credentials are stored in the `forgejo-runner-data` volume. The
bootstrap script uses the configured administrator credential transiently, then
preserves an existing `WAMA_APPS_DEPLOY_ROOT` and requires its
`.wama-forgejo-applications-root` marker before the application deployment
helper will copy an application checkout there. It never pushes, clones, or
otherwise uses the parent infrastructure Git repository.