# PostgreSQL

`postgres` provides a persistent database for the root-owned `blobmeta-catalog`
projection of compacted `Blobmeta` records. The catalog creates only its
immutable `blobmeta_catalog` schema, which stores session/artifact metadata,
status, request integrity, and per-MRID row coverage. Kafka remains the source
of truth; individual measurement rows stay in Druid and SeaweedFS.

The service initializes the `wama` database and role on its first start, then
persists them in the root `postgres-data` volume. It listens on port 5432 on all
host interfaces and at `postgres:5432` on the Compose network.

| Setting | Value |
| --- | --- |
| Database | `wama` |
| User | `wama` |
| Password | `wama-postgres-password` |

These intentionally public credentials are for this trusted local PoC only and
must not be reused elsewhere. A future Kafka Connect mirror of `Masterdata` and
`Schema` remains separate from the implemented Blobmeta materializer.

Check the initialized database:

```sh
docker compose exec -T postgres \
  psql -U wama -d wama -c 'SELECT current_database(), current_user;'
```