#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
probe_log="$(mktemp)"
probe_pid=""

cd "$repository_root"

cleanup() {
  if [[ -n "$probe_pid" ]] && kill -0 "$probe_pid" 2>/dev/null; then
    kill "$probe_pid" 2>/dev/null || true
    wait "$probe_pid" 2>/dev/null || true
  fi
  rm -f "$probe_log"
}

show_failure_diagnostics() {
  printf '%s\n' "IEC 104 browser validation failed; leaving the stack running for inspection."
  docker compose ps --all || true
  docker compose logs --tail 200 \
    iec104-browser \
    iec104-exporter \
    iec104-receiver \
    kafka \
    kafka-init || true
  [[ -s "$probe_log" ]] && cat "$probe_log" || true
}

trap cleanup EXIT
trap show_failure_diagnostics ERR

docker compose stop iec104-browser >/dev/null 2>&1 || true
docker compose up -d --build --wait iec104-browser
docker compose --profile iec104-test build iec104-receiver

docker compose exec -T iec104-browser \
  python -m iec104_browser.e2e >"$probe_log" 2>&1 &
probe_pid="$!"

# Export records remain in Kafka until the browser control center completes STARTDT.
# The WebSocket probe proves that activation by receiving every fixture ASDU.
IEC104_RECEIVER_MODE=publish-only \
  docker compose --profile iec104-test run --rm --no-deps iec104-receiver
wait "$probe_pid"
probe_pid=""
cat "$probe_log"

deadline=$((SECONDS + ${IEC104_BROWSER_E2E_TIMEOUT_SECONDS:-30}))
while ((SECONDS < deadline)); do
  if curl --fail --silent http://127.0.0.1:${IEC104_BROWSER_PORT:-3003}/v1/iec104/status \
    | grep --quiet '"active":false.*"state":"idle".*"viewers":0'; then
    break
  fi
  sleep 1
done
if ((SECONDS >= deadline)); then
  printf '%s\n' "IEC 104 browser retained its control-center connection after stream closure." >&2
  exit 1
fi

trap - ERR
printf '%s\n' "IEC 104 browser live-stream validation passed."