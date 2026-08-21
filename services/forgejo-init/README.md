# Forgejo bootstrap

`forgejo-init` runs after Forgejo becomes healthy. It creates the configured
administrator, bootstraps the one separate `gateway-c37-118-onboarding`
repository, and initializes the root-owned processor registry. On its first
run, the registry validates the four approved processor seeds and ensures one
private repository, marker-owned child root, and CI/deploy connection pair for
each. Later runs reconcile only the persisted registry; a folder appearing
under `forgejo-repos/` never self-registers. Existing nonempty private
repositories are left unchanged and are never reseeded.

Configure the values in the root `.env`; a duplicate
[forgejo-init.env.example](forgejo-init.env.example) is available as a focused
reference. `FORGEJO_RUNNER_URL` must be reachable from the runner and its job
containers. For CI/CD, set it to the same HTTPS endpoint as `FORGEJO_ROOT_URL`.

Runner credentials and a `write:package` token for the configured administrator
are stored only in the `forgejo-runner-data` volume. The registry renders
connections and exact active child roots atomically. The generic labels remain
`wama-processors-ci` and `wama-processors-deploy`; connection names include the
owning repository. The separate gateway onboarding connections and deployment
root stay outside the processor registry.

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

`WAMA_PROCESSOR_DEPLOY_BASE_ROOT` and
`WAMA_GATEWAY_C37_118_ONBOARDING_DEPLOY_ROOT` must be absolute and not `/`.
Each registered processor receives only
`$WAMA_PROCESSOR_DEPLOY_BASE_ROOT/<processor-name>`, with a
`.wama-forgejo-processor-root` marker; the onboarding root retains its separate
gateway marker. Registry removal stops only the application-local Compose
project, removes its marked child root and project volumes, and preserves the
private Forgejo repository and package history.

Use the root-only administration wrapper to inspect, register, or unregister a
validated processor seed. It runs inside the trusted init container and never
becomes available to a processor workflow:

```sh
scripts/wama-processor-admin.sh status
scripts/wama-processor-admin.sh register processor-example
scripts/wama-processor-admin.sh deploy-existing processor-example
scripts/wama-processor-admin.sh unregister processor-example
```

Run the focused bootstrap safety test from the infrastructure repository root:

```sh
sh services/forgejo-init/tests/test_bootstrap_processors.sh
sh services/forgejo-init/tests/test_gateway_onboarding_agent_credentials.sh
python3 services/forgejo-init/tests/test_processor_registry.py -v
```