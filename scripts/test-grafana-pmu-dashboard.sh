#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
wait_timeout_seconds="${GRAFANA_PMU_DASHBOARD_TIMEOUT_SECONDS:-300}"

cd "$repository_root"

show_failure_diagnostics() {
  printf '%s\n' "Grafana PMU-dashboard validation failed; leaving the stack running for inspection."
  docker compose ps --all || true
  docker compose logs --tail 200 \
    grafana \
    druid \
    druid-init \
    kafka \
    kafka-init \
    pmu-gateway || true
}

trap show_failure_diagnostics ERR

docker compose config --quiet
docker compose build grafana druid druid-init infra-readiness trino-session-init
docker compose up -d --wait --wait-timeout "$wait_timeout_seconds" trino-session-writer
docker compose run --rm --build --no-deps trino-session-init
docker compose up -d --wait --wait-timeout "$wait_timeout_seconds" \
  pmu-gateway \
  druid \
  grafana
docker compose run --rm --no-deps druid-init
docker compose run --rm --no-deps infra-readiness \
  python -c 'from infra_readiness.checks import check_druid, check_grafana; from infra_readiness.config import Settings; settings = Settings.from_environment(); check_druid(settings); check_grafana(settings)'

trap - ERR
printf '%s\n' "Grafana PMU-dashboard validation passed."