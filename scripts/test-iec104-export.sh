#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
browser_stopped=false

cd "$repository_root"

wait_for_browser_health() {
  local deadline=$((SECONDS + ${IEC104_BROWSER_START_TIMEOUT_SECONDS:-60}))

  while ((SECONDS < deadline)); do
    if curl --fail --silent \
      http://127.0.0.1:${IEC104_BROWSER_PORT:-3003}/healthz >/dev/null; then
      return 0
    fi
    sleep 1
  done

  printf '%s\n' "Timed out waiting for the IEC 104 browser health endpoint." >&2
  return 1
}

restore_browser() {
  if [[ "$browser_stopped" == true ]]; then
    docker compose up -d --build iec104-browser || true
    wait_for_browser_health || true
  fi
}

show_failure_diagnostics() {
  printf '%s\n' "IEC 104 export validation failed; leaving the stack running for inspection."
  docker compose ps --all || true
  docker compose logs --tail 200 \
    iec104-exporter \
    iec104-receiver \
    kafka \
    kafka-init || true
}

trap show_failure_diagnostics ERR
trap restore_browser EXIT

docker compose stop iec104-browser >/dev/null 2>&1 || true
browser_stopped=true
docker compose up -d --build --wait iec104-exporter
docker compose --profile iec104-test run --rm --build iec104-receiver
docker compose up -d --build iec104-browser
wait_for_browser_health
browser_stopped=false

trap - ERR
trap - EXIT
printf '%s\n' "IEC 104 Kafka-to-control-center validation passed."