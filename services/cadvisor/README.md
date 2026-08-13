# cAdvisor

`cadvisor` provides Docker-container CPU, memory, and network metrics to
VictoriaMetrics on the internal Compose network. It has no published host port
and reports Compose labels used by the provisioned Grafana dashboard. Disk
metrics are disabled because this Docker 29 host uses the containerd
snapshotter.

The container is privileged and mounts Docker and host paths, including
`/var/run` read-write, to inspect running containers. Use it only on the trusted
PoC host.