# Forgejo bootstrap

`forgejo-init` runs after Forgejo becomes healthy. It creates the configured
administrator and ensures five private repositories through the Forgejo API:
`processor-frequency-scale`, `processor-apparent-power`, and
`processor-frequency-iec104-export`, `processor-lfr-frequency-provision`, and
`gateway-c37-118-onboarding`.
Each tracked seed is pushed to `main` only when its remote has no refs. An
existing nonempty private repository is left unchanged; an existing nonprivate
repository makes bootstrap fail without changing it.

Configure the values in the root `.env`; a duplicate
[forgejo-init.env.example](forgejo-init.env.example) is available as a focused
reference. `FORGEJO_RUNNER_URL` must be reachable from the runner and its job
containers. For CI/CD, set it to the same HTTPS endpoint as `FORGEJO_ROOT_URL`.

Runner credentials and a `write:package` token for the configured administrator
are stored only in the `forgejo-runner-data` volume. The bootstrap script uses
the administrator credential transiently, creates the scoped package token only
when its runner-volume file is absent, and registers separate CI and deployment
connections for all five repositories on the one runner daemon. The generic
labels remain `wama-processors-ci` and `wama-processors-deploy`; runner
registration names include the owning repository.

Bootstrap also creates or validates a restricted, non-admin
`FORGEJO_GATEWAY_C37_118_ONBOARDING_AGENT_USERNAME` collaborator with `write`
access only to the existing private `gateway-c37-118-onboarding` repository.
Its generated API token remains in `forgejo-runner-data`; it is never placed in
`.env`, Git remotes, or workflow environments. From the infrastructure root,
install it only into the separate private checkout:

```sh
sh scripts/configure-forgejo-gateway-onboarding-agent.sh \
	--checkout ../gateway-c37-118-onboarding
```

The installer rejects the parent checkout and tracked seed, preserves `origin`,
and writes a mode-`0600` local credential file. Use
`scripts/with-forgejo-gateway-onboarding-agent.sh` for REST calls so the token
is exported only to the invoked command. Re-run the installer after
`docker compose down -v`, which intentionally removes the generated token.

`WAMA_FREQUENCY_SCALE_DEPLOY_ROOT`, `WAMA_APPARENT_POWER_DEPLOY_ROOT`, and
`WAMA_FREQUENCY_IEC104_EXPORT_DEPLOY_ROOT`, and
`WAMA_LFR_FREQUENCY_PROVISION_DEPLOY_ROOT`, and
`WAMA_GATEWAY_C37_118_ONBOARDING_DEPLOY_ROOT` must be absolute and not `/`.
Bootstrap creates `.wama-forgejo-processor-root` only in new or empty processor
roots and `.wama-forgejo-gateway-onboarding-root` only in the onboarding root;
it rejects every unmarked nonempty path. A repository deployment helper copies
only its own tracked files into its matching root. Bootstrap never pushes,
clones, or otherwise uses the parent infrastructure Git repository.

Run the focused bootstrap safety test from the infrastructure repository root:

```sh
sh services/forgejo-init/tests/test_bootstrap_processors.sh
sh services/forgejo-init/tests/test_gateway_onboarding_agent_credentials.sh
```