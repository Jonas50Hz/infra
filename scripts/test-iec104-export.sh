#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
browser_stopped=false

cd "$repository_root"

restore_browser() {
  if [[ "$browser_stopped" == true ]]; then
    docker compose up -d --build --wait iec104-browser || true
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
restore_browser
browser_stopped=false

trap - ERR
trap - EXIT
printf '%s\n' "IEC 104 Kafka-to-control-center validation passed."