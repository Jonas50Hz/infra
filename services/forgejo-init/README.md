# Forgejo bootstrap

`forgejo-init` runs after Forgejo becomes healthy. It creates the configured
administrator, ensures a private `wama-processors` repository through the
Forgejo API, and seeds the tracked processors repository on `main` only when
the remote has no refs. An existing nonempty private repository is left
unchanged; an existing nonprivate repository makes bootstrap fail without
changing it.

Configure the values in the root `.env`; a duplicate
[forgejo-init.env.example](forgejo-init.env.example) is available as a focused
reference. `FORGEJO_RUNNER_URL` must be reachable from the runner and its job
containers. For CI/CD, set it to the same HTTPS endpoint as `FORGEJO_ROOT_URL`.

Runner credentials and a `write:package` token for the configured administrator
are stored only in the `forgejo-runner-data` volume. The bootstrap script uses
the administrator credential transiently, creates the scoped package token only
when its runner-volume file is absent, and registers `wama-processors-ci` plus
`wama-processors-deploy` as separate repository-scoped connections on the one
runner daemon.

`WAMA_PROCESSORS_DEPLOY_ROOT` must be absolute and not `/`. Bootstrap creates
its `.wama-forgejo-processors-root` marker only in a new or empty directory and
rejects an unmarked nonempty path. The deployment helper copies only tracked
processors files into that marked root. Bootstrap never pushes, clones, or
otherwise uses the parent infrastructure Git repository.

Run the focused bootstrap safety test from the infrastructure repository root:

```sh
sh services/forgejo-init/tests/test_bootstrap.sh
```