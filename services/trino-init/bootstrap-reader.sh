#!/usr/bin/env bash

set -euo pipefail

reader_role="trino_blobmeta_reader"
session_reader_role="trino_session_reader"
session_writer_role="trino_session_writer"
owner_role="wama"
required_tables="session_blobs,session_blob_mrids"
timeout_seconds="${TRINO_INIT_TIMEOUT_SECONDS:-180}"
reader_password="${TRINO_BLOBMETA_READER_PASSWORD:-}"
session_reader_password="${TRINO_SESSION_READER_PASSWORD:-}"
session_writer_password="${TRINO_SESSION_WRITER_PASSWORD:-}"

if [[ ! "$timeout_seconds" =~ ^[1-9][0-9]*$ ]]; then
  printf 'TRINO_INIT_TIMEOUT_SECONDS must be a positive integer; found %s\n' "$timeout_seconds" >&2
  exit 2
fi

if [[ -z "$reader_password" ]]; then
  printf '%s\n' "TRINO_BLOBMETA_READER_PASSWORD must not be empty" >&2
  exit 2
fi

if [[ -z "$session_reader_password" ]]; then
  printf '%s\n' "TRINO_SESSION_READER_PASSWORD must not be empty" >&2
  exit 2
fi

if [[ -z "$session_writer_password" ]]; then
  printf '%s\n' "TRINO_SESSION_WRITER_PASSWORD must not be empty" >&2
  exit 2
fi

wait_for_catalog() {
  local deadline=$((SECONDS + timeout_seconds))

  while ((SECONDS < deadline)); do
    local table_count
    if table_count="$(
      psql --no-psqlrc --tuples-only --no-align --quiet \
        --command "
          SELECT count(*)
          FROM information_schema.tables
          WHERE table_schema = 'blobmeta_catalog'
            AND table_name IN ('session_blobs', 'session_blob_mrids');
        " 2>/dev/null
    )" && [[ "$table_count" == "2" ]]; then
      return 0
    fi
    sleep 2
  done

  printf '%s\n' "Timed out waiting for blobmeta_catalog tables: $required_tables" >&2
  return 1
}

wait_for_catalog

psql --no-psqlrc --set=ON_ERROR_STOP=1 \
  --set=reader_role="$reader_role" \
  --set=reader_password="$reader_password" \
  --set=session_reader_role="$session_reader_role" \
  --set=session_reader_password="$session_reader_password" \
  --set=session_writer_role="$session_writer_role" \
  --set=session_writer_password="$session_writer_password" \
  --set=owner_role="$owner_role" <<'SQL'
SELECT format(
    'CREATE ROLE %I LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS',
    :'reader_role',
    :'reader_password'
)
WHERE NOT EXISTS (
    SELECT 1
    FROM pg_roles
    WHERE rolname = :'reader_role'
) \gexec

ALTER ROLE :"reader_role" LOGIN PASSWORD :'reader_password' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;

SELECT format(
  'CREATE ROLE %I LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS',
  :'session_reader_role',
  :'session_reader_password'
)
WHERE NOT EXISTS (
  SELECT 1
  FROM pg_roles
  WHERE rolname = :'session_reader_role'
) \gexec

ALTER ROLE :"session_reader_role" LOGIN PASSWORD :'session_reader_password' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;

SELECT format(
  'CREATE ROLE %I LOGIN PASSWORD %L NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS',
  :'session_writer_role',
  :'session_writer_password'
)
WHERE NOT EXISTS (
  SELECT 1
  FROM pg_roles
  WHERE rolname = :'session_writer_role'
) \gexec

ALTER ROLE :"session_writer_role" LOGIN PASSWORD :'session_writer_password' NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;

REVOKE ALL PRIVILEGES ON DATABASE :"DBNAME" FROM :"reader_role";
GRANT CONNECT ON DATABASE :"DBNAME" TO :"reader_role";
REVOKE ALL PRIVILEGES ON SCHEMA blobmeta_catalog FROM :"reader_role";
GRANT USAGE ON SCHEMA blobmeta_catalog TO :"reader_role";
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA blobmeta_catalog FROM :"reader_role";
GRANT SELECT ON ALL TABLES IN SCHEMA blobmeta_catalog TO :"reader_role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner_role" IN SCHEMA blobmeta_catalog REVOKE ALL ON TABLES FROM :"reader_role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner_role" IN SCHEMA blobmeta_catalog GRANT SELECT ON TABLES TO :"reader_role";

CREATE SCHEMA IF NOT EXISTS iceberg_catalog AUTHORIZATION :"owner_role";
ALTER SCHEMA iceberg_catalog OWNER TO :"owner_role";

CREATE TABLE IF NOT EXISTS iceberg_catalog.iceberg_tables (
  catalog_name varchar(255) NOT NULL,
  table_namespace varchar(255) NOT NULL,
  table_name varchar(255) NOT NULL,
  metadata_location varchar(1000),
  previous_metadata_location varchar(1000),
  iceberg_type varchar(5),
  PRIMARY KEY (catalog_name, table_namespace, table_name)
);

CREATE TABLE IF NOT EXISTS iceberg_catalog.iceberg_namespace_properties (
  catalog_name varchar(255) NOT NULL,
  namespace varchar(255) NOT NULL,
  property_key varchar(255),
  property_value varchar(1000),
  PRIMARY KEY (catalog_name, namespace, property_key)
);

REVOKE ALL PRIVILEGES ON DATABASE :"DBNAME" FROM :"session_reader_role";
GRANT CONNECT ON DATABASE :"DBNAME" TO :"session_reader_role";
REVOKE CREATE ON DATABASE :"DBNAME" FROM :"session_reader_role";
REVOKE ALL PRIVILEGES ON SCHEMA iceberg_catalog FROM :"session_reader_role";
GRANT USAGE ON SCHEMA iceberg_catalog TO :"session_reader_role";
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA iceberg_catalog FROM :"session_reader_role";
GRANT SELECT ON ALL TABLES IN SCHEMA iceberg_catalog TO :"session_reader_role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner_role" IN SCHEMA iceberg_catalog REVOKE ALL ON TABLES FROM :"session_reader_role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner_role" IN SCHEMA iceberg_catalog GRANT SELECT ON TABLES TO :"session_reader_role";

REVOKE ALL PRIVILEGES ON DATABASE :"DBNAME" FROM :"session_writer_role";
GRANT CONNECT ON DATABASE :"DBNAME" TO :"session_writer_role";
REVOKE CREATE ON DATABASE :"DBNAME" FROM :"session_writer_role";
REVOKE ALL PRIVILEGES ON SCHEMA iceberg_catalog FROM :"session_writer_role";
GRANT USAGE ON SCHEMA iceberg_catalog TO :"session_writer_role";
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA iceberg_catalog FROM :"session_writer_role";
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA iceberg_catalog TO :"session_writer_role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner_role" IN SCHEMA iceberg_catalog REVOKE ALL ON TABLES FROM :"session_writer_role";
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner_role" IN SCHEMA iceberg_catalog GRANT SELECT, INSERT, UPDATE ON TABLES TO :"session_writer_role";
SQL

printf '%s\n' "Trino Blobmeta and Iceberg catalog roles are ready."