# Alerta PostgreSQL

`alerta-postgres` is the isolated durable PostgreSQL backend for the
root-owned Alerta service. It deliberately does not reuse the root `postgres`
service or expose a host port.

The image is pinned to
`postgres:17.6@sha256:00bc86618629af00d2937fdc5a5d63db3ff8450acf52f0636ec813c7f4902929`.
The digest was verified by a local `docker pull`; PostgreSQL 17.6 is within
Alerta's supported PostgreSQL 13+ range. Its state is retained only in the
root `alerta-postgres-data` volume.

The tracked credentials are intentionally local-PoC-only values. They must not
be reused outside this Compose environment.