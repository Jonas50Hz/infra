#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
complete_session_id="${TRINO_FEDERATION_COMPLETE_ID:-$(cat /proc/sys/kernel/random/uuid)}"
partial_session_id="${TRINO_FEDERATION_PARTIAL_ID:-$(cat /proc/sys/kernel/random/uuid)}"

cd "$repository_root"

validate_session_id() {
  local name="$1"
  local value="$2"

  if [[ ! "$value" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]; then
    printf '%s must be a lowercase canonical UUID\n' "$name" >&2
    exit 2
  fi
}

validate_session_id "TRINO_FEDERATION_COMPLETE_ID" "$complete_session_id"
validate_session_id "TRINO_FEDERATION_PARTIAL_ID" "$partial_session_id"

if [[ "$complete_session_id" == "$partial_session_id" ]]; then
  printf '%s\n' "TRINO_FEDERATION_COMPLETE_ID and TRINO_FEDERATION_PARTIAL_ID must differ" >&2
  exit 2
fi

show_failure_diagnostics() {
  printf '%s\n' "Trino federation validation failed; leaving the stack running for inspection."
  docker compose ps --all || true
  docker compose logs --tail 200 \
    trino \
    trino-init \
    druid \
    druid-init \
    postgres \
    measurement-session-processor \
    blobmeta-catalog \
    measurement-session-e2e || true
}

trap show_failure_diagnostics ERR

docker compose config --quiet
MEASUREMENT_SESSION_COMPLETE_ID="$complete_session_id" \
  MEASUREMENT_SESSION_PARTIAL_ID="$partial_session_id" \
  scripts/test-measurement-session-flow.sh
docker compose run --rm --no-deps trino-init
docker compose up -d --no-deps --wait trino
docker compose run --rm --build --no-deps \
  --env TRINO_COMPLETE_SESSION_ID="$complete_session_id" \
  --volume "$repository_root/services/trino/tests:/opt/wama/trino-tests:ro" \
  infra-readiness \
  python /opt/wama/trino-tests/verify_federation.py

trap - ERR
printf 'Trino Druid/PostgreSQL federation validation passed for %s.\n' "$complete_session_id"