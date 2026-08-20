#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
complete_session_id="${MEASUREMENT_SESSION_COMPLETE_ID:-$(cat /proc/sys/kernel/random/uuid)}"
partial_session_id="${MEASUREMENT_SESSION_PARTIAL_ID:-$(cat /proc/sys/kernel/random/uuid)}"

cd "$repository_root"

validate_session_id() {
  local name="$1"
  local value="$2"

  if [[ ! "$value" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]; then
    printf '%s must be a lowercase canonical UUID\n' "$name" >&2
    exit 2
  fi
}

validate_session_id "MEASUREMENT_SESSION_COMPLETE_ID" "$complete_session_id"
validate_session_id "MEASUREMENT_SESSION_PARTIAL_ID" "$partial_session_id"

if [[ "$complete_session_id" == "$partial_session_id" ]]; then
  printf '%s\n' "MEASUREMENT_SESSION_COMPLETE_ID and MEASUREMENT_SESSION_PARTIAL_ID must differ" >&2
  exit 2
fi

export MEASUREMENT_SESSION_COMPLETE_ID="$complete_session_id"
export MEASUREMENT_SESSION_PARTIAL_ID="$partial_session_id"

show_failure_diagnostics() {
  printf '%s\n' "Measurement-session request-to-Blobmeta validation failed; leaving the stack running for inspection."
  docker compose ps --all || true
  docker compose logs --tail 200 \
    measurement-session-processor \
    blobmeta-catalog \
    measurement-session-e2e \
    kafka \
    postgres \
    seaweedfs \
    druid \
    druid-init || true
}

trap show_failure_diagnostics ERR

docker compose config --quiet
docker compose up -d --build --wait \
  measurement-session-processor \
  blobmeta-catalog
docker compose --profile measurement-session-test run --rm --build measurement-session-e2e

trap - ERR
printf 'Measurement-session request-to-Blobmeta validation passed for %s and %s.\n' \
  "$complete_session_id" "$partial_session_id"