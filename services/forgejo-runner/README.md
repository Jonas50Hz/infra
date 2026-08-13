# Forgejo Actions runner

`forgejo-runner` builds `wama-forgejo-runner:local`, which contains Forgejo
Runner plus Git, Node, Python 3.12, Protocol Buffer tooling, Docker CLI,
Docker Compose, and rsync. The image is also the default container for the
`wama-ci` Actions label.

The bootstrap service registers two labels on the same repository-scoped runner:

- `wama-ci` runs normal workflow jobs in `wama-forgejo-runner:local`.
- `wama-deploy` runs the serialized deploy job in the runner container so the
  absolute deployment root remains visible to the host Docker daemon.

The service mounts `/var/run/docker.sock` and `WAMA_DEPLOY_ROOT`. This grants
workflow code broad control of the Docker host. Limit repository write access to
trusted users and do not reuse this configuration outside the local PoC.