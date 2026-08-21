#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

cd "$repository_root"

show_failure_diagnostics() {
  printf '%s\n' "Druid PMU-ingestion validation failed; leaving the stack running for inspection."
  docker compose ps --all || true
  docker compose logs --tail 200 \
    druid \
    druid-init \
    kafka \
    kafka-init || true
}

trap show_failure_diagnostics ERR

docker compose config --quiet
docker compose build druid druid-init infra-readiness
docker compose up -d --wait druid
docker compose run --rm --no-deps druid-init
docker compose run --rm --no-deps infra-readiness \
  python -c 'from infra_readiness.config import Settings; from infra_readiness.druid import check_druid; check_druid(Settings.from_environment())'

trap - ERR
printf '%s\n' "Druid PMU-ingestion validation passed."