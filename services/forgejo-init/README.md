# Forgejo bootstrap

`forgejo-init` runs after Forgejo becomes healthy. It creates the configured
administrator and ensures two private repositories through the Forgejo API:
`processor-frequency-scale` and `processor-apparent-power`. Each tracked seed
is pushed to `main` only when its remote has no refs. An existing nonempty
private repository is left unchanged; an existing nonprivate repository makes
bootstrap fail without changing it.

Configure the values in the root `.env`; a duplicate
[forgejo-init.env.example](forgejo-init.env.example) is available as a focused
reference. `FORGEJO_RUNNER_URL` must be reachable from the runner and its job
containers. For CI/CD, set it to the same HTTPS endpoint as `FORGEJO_ROOT_URL`.

Runner credentials and a `write:package` token for the configured administrator
are stored only in the `forgejo-runner-data` volume. The bootstrap script uses
the administrator credential transiently, creates the scoped package token only
when its runner-volume file is absent, and registers separate CI and deployment
connections for both repositories on the one runner daemon. The generic labels
remain `wama-processors-ci` and `wama-processors-deploy`; runner registration
names include the owning repository.

`WAMA_FREQUENCY_SCALE_DEPLOY_ROOT` and `WAMA_APPARENT_POWER_DEPLOY_ROOT` must
be absolute and not `/`. Bootstrap creates a
`.wama-forgejo-processor-root` marker only in each new or empty directory and
rejects an unmarked nonempty path. A repository deployment helper copies only
its own tracked files into its matching root. Bootstrap never pushes, clones,
or otherwise uses the parent infrastructure Git repository.

Run the focused bootstrap safety test from the infrastructure repository root:

```sh
sh services/forgejo-init/tests/test_bootstrap_processors.sh
```