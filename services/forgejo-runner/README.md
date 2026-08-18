# Forgejo Actions runner

`forgejo-runner` builds `wama-forgejo-runner:local`, which contains Forgejo
Runner plus Git, Node, Python 3.12, Protocol Buffer tooling, Docker CLI,
Docker Compose, and rsync. The image is also the default container for the
`wama-processors-ci` Actions label.

The bootstrap service registers two repository-scoped connections on the same
capacity-one runner daemon:

- `wama-processors-ci` runs processors-repository workflow jobs in
  `wama-forgejo-runner:local`.
- `wama-processors-deploy` runs the serialized processors deployment job in the
  runner container so the absolute processors deployment root remains visible
  to the host Docker daemon.

The service mounts `/var/run/docker.sock` and `WAMA_PROCESSORS_DEPLOY_ROOT`.
This grants processors workflow code broad control of the Docker host. Limit
the separate `wama-processors` repository to trusted users and do not reuse
this configuration outside the local PoC. Bootstrap also injects a dedicated
`write:package` token into these trusted jobs so they can publish and pull the
private Forgejo processor images.