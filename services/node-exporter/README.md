# Node Exporter

`node-exporter` provides Linux host CPU, memory, filesystem, and network
metrics to VictoriaMetrics on the internal Compose network. It has no published
host port.

The service uses the host PID namespace and read-only mounts of the Docker
host root, `/proc`, and `/sys`; it deliberately does not require recursive bind
propagation so the local Compose PoC also starts on Docker Desktop. On macOS,
the observed host is Docker Desktop's Linux VM rather than the macOS host.