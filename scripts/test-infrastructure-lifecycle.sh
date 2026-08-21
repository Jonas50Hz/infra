#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readiness_timeout_seconds="${INFRA_LIFECYCLE_TIMEOUT_SECONDS:-240}"

cd "$repository_root"

show_failure_diagnostics() {
  printf '%s\n' "Infrastructure lifecycle validation failed; leaving the stack running for inspection."
  docker compose ps --all || true
  docker compose logs --tail 200 \
    infra-readiness \
    kafka \
    kafka-init \
    kafka-exporter \
    pmu-gateway \
    iec104-exporter \
    iec104-browser \
    druid \
    druid-init \
    postgres \
    trino \
    trino-init \
    seaweedfs \
    forgejo \
    forgejo-init \
    forgejo-runner \
    victoria-metrics \
    grafana || true
}

wait_for_readiness() {
  local phase="$1"
  local deadline=$((SECONDS + readiness_timeout_seconds))

  while ((SECONDS < deadline)); do
    local container_id
    container_id="$(docker compose ps --all --quiet infra-readiness)"
    if [[ -n "$container_id" ]]; then
      local state
      state="$(docker inspect --format '{{.State.Status}} {{.State.ExitCode}}' "$container_id" 2>/dev/null || true)"
      case "$state" in
        "exited 0")
          printf '%s\n' "Infrastructure readiness passed after $phase."
          return 0
          ;;
        exited\ *)
          printf '%s\n' "Infrastructure readiness failed after $phase: $state" >&2
          return 1
          ;;
      esac
    fi
    sleep 2
  done

  printf '%s\n' "Timed out waiting for infrastructure readiness after $phase." >&2
  return 1
}

verify_druid_live_measurement() {
  docker compose run --rm --no-deps infra-readiness \
    python -c 'from infra_readiness.config import Settings; from infra_readiness.druid import check_druid; check_druid(Settings.from_environment())'
}

start_and_verify() {
  local phase="$1"

  docker compose up -d
  wait_for_readiness "$phase"
  verify_druid_live_measurement
}

sh services/forgejo-init/tests/test_bootstrap_processors.sh
docker compose config --quiet
docker compose down -v --remove-orphans

if ! start_and_verify "a clean-volume start"; then
  show_failure_diagnostics
  exit 1
fi

docker compose down

if ! start_and_verify "a persistent-volume restart"; then
  show_failure_diagnostics
  exit 1
fi

printf '%s\n' "Infrastructure lifecycle validation passed; the restarted stack remains running."