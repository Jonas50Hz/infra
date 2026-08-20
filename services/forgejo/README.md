# Forgejo

`forgejo` provides the local Git forge, Forgejo Actions control plane, and
built-in OCI package registry. The service listens on port 3000 internally;
an HTTPS endpoint outside this Compose fragment must route to it when image
publication and Actions checkout are used.

Set `FORGEJO_DOMAIN` and `FORGEJO_ROOT_URL` in the root `.env` file to that
endpoint. `LOCAL_ROOT_URL` stays on the Compose network so Forgejo can make
internal calls without crossing the reverse proxy. Package registry support is
explicitly enabled and repositories are forced private for this trusted PoC.

The one-shot [`../forgejo-init/`](../forgejo-init/) service creates the initial
administrator, private seeded `processor-frequency-scale`,
`processor-apparent-power`, `processor-frequency-iec104-export`, and
`processor-lfr-frequency-provision` repositories, and separate CI/deployment
runner connections scoped to each repository. The parent infrastructure
repository is never pushed to this Forgejo instance.