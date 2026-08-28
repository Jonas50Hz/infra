# Forgejo Actions runner

`forgejo-runner` builds `wama-forgejo-runner:local`, which contains Forgejo
Runner plus Git, Node, Python 3.12, Protocol Buffer tooling, Docker CLI,
Docker Compose, and rsync. The image is the default container for the
`wama-processors-ci` Actions label.

The bootstrap service registers twelve repository-scoped connections on the same
capacity-one runner daemon:

- `wama-processor-frequency-scale-ci` and
  `wama-processor-apparent-power-ci` run validation and publication jobs.
- `wama-processor-frequency-scale-deploy` and
  `wama-processor-apparent-power-deploy` run their serialized deployment jobs
  with only their matching deployment root visible to the host Docker daemon.
- `wama-processor-frequency-iec104-export-ci` and
  `wama-processor-frequency-iec104-export-deploy` validate/publish and deploy
  only the direct frequency IEC 104 processor.
- `wama-processor-alarm-threshold-ci` and
  `wama-processor-alarm-threshold-deploy` validate/publish and deploy only the
  alarm-threshold processor.
- `wama-processor-frequency-measurement-session-ci` and
  `wama-processor-frequency-measurement-session-deploy` validate/publish and
  deploy only the frequency-capture MeasurementSession processor.
- `wama-gateway-c37-118-ci` and
  `wama-gateway-c37-118-deploy` validate/publish the combined image,
  run the one-shot C37.118 Masterdata publisher, and reconcile only
  catalog-derived legacy-v2 adapters in their marker-owned deployment root.

The service mounts `/var/run/docker.sock` plus all individual managed roots.
This grants trusted workflow code broad control of the Docker host. Limit write
access to all managed repositories and do not reuse this
configuration outside the local PoC. Bootstrap injects one dedicated
`write:package` token into trusted jobs so they can publish and pull private
Forgejo images.