# Forgejo Actions runner

`forgejo-runner` builds `wama-forgejo-runner:local`, which contains Forgejo
Runner plus Git, Node, Python 3.12, Protocol Buffer tooling, Docker CLI,
Docker Compose, and rsync. The image is the default container for the
`wama-processors-ci` Actions label.

The bootstrap service registers four repository-scoped connections on the same
capacity-one runner daemon:

- `wama-processor-frequency-scale-ci` and
  `wama-processor-apparent-power-ci` run validation and publication jobs.
- `wama-processor-frequency-scale-deploy` and
  `wama-processor-apparent-power-deploy` run their serialized deployment jobs
  with only their matching deployment root visible to the host Docker daemon.

The service mounts `/var/run/docker.sock` plus both individual processor roots.
This grants trusted processor workflow code broad control of the Docker host.
Limit write access to both processor repositories and do not reuse this
configuration outside the local PoC. Bootstrap injects one dedicated
`write:package` token into trusted jobs so they can publish and pull private
Forgejo processor images.