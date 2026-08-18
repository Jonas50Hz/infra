#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
session_id="${MEASUREMENT_SESSION_ID_OVERRIDE:-$(cat /proc/sys/kernel/random/uuid)}"

cd "$repository_root"

if [[ ! "$session_id" =~ ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$ ]]; then
  printf '%s\n' "MEASUREMENT_SESSION_ID_OVERRIDE must be a lowercase canonical UUID" >&2
  exit 2
fi

export MEASUREMENT_SESSION_ID_OVERRIDE="$session_id"

show_failure_diagnostics() {
  printf '%s\n' "Measurement-session contract-to-download validation failed; leaving the stack running for inspection."
  docker compose ps --all || true
  docker compose logs --tail 200 \
    measurement-session-exporter \
    measurement-session-api \
    measurement-session-browser \
    measurement-session-e2e \
    kafka \
    postgres \
    seaweedfs || true
}

trap show_failure_diagnostics ERR

docker compose up -d --build --wait \
  measurement-session-api \
  measurement-session-browser
docker compose --profile measurement-session-export run --rm --build measurement-session-exporter
docker compose --profile measurement-session-test run --rm --build measurement-session-e2e

trap - ERR
printf 'Measurement-session contract-to-download validation passed for %s.\n' "$session_id"