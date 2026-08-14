# PostgreSQL

`postgres` provides a persistent, initially empty database target for the
future Kafka Connect mirror of the compacted `Masterdata`, `Schema`, and
`Blobmeta` topics. Kafka remains the source of truth until that connector is
introduced.

The service initializes the `wama` database and role on its first start, then
persists them in the root `postgres-data` volume. It listens on port 5432 on all
host interfaces and at `postgres:5432` on the Compose network.

| Setting | Value |
| --- | --- |
| Database | `wama` |
| User | `wama` |
| Password | `wama-postgres-password` |

These intentionally public credentials are for this trusted local PoC only and
must not be reused elsewhere. No application tables, schemas, Kafka Connect
configuration, or data synchronization are created by this service.

Check the initialized database:

```sh
docker compose exec -T postgres \
  psql -U wama -d wama -c 'SELECT current_database(), current_user;'
```