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
administrator, private repository, and repository-scoped runner registration.