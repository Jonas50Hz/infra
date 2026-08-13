# Node Exporter

`node-exporter` provides Linux host CPU, memory, filesystem, and network
metrics to VictoriaMetrics on the internal Compose network. It has no published
host port.

The service uses the host PID namespace and read-only mounts of the host root,
`/proc`, and `/sys`. It is therefore intended only for this trusted Linux
Docker host.