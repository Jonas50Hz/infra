# Forgejo Actions runner

`forgejo-runner` builds `wama-forgejo-runner:local`, which contains Forgejo
Runner plus Git, Node, Python 3.12, Protocol Buffer tooling, Docker CLI,
Docker Compose, and rsync. The image is also the default container for the
`wama-app-ci` Actions label.

The bootstrap service registers two labels on the same repository-scoped runner:

- `wama-app-ci` runs application-repository workflow jobs in
  `wama-forgejo-runner:local`.
- `wama-app-deploy` runs the serialized application deployment job in the
  runner container so the absolute application deployment root remains visible
  to the host Docker daemon.

The service mounts `/var/run/docker.sock` and `WAMA_APPS_DEPLOY_ROOT`. This
grants application workflow code broad control of the Docker host. Limit the
separate `wama-applications` repository to trusted users and do not reuse this
configuration outside the local PoC.