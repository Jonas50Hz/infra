#!/usr/bin/env bash

# End-to-end validation of the Berlin weather map: Open-Meteo collection through
# Kafka and Druid into both provisioned geomap panels.

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
processor_repository="$repository_root/forgejo-repos/processor-weather-map"
dashboard_uid=wama-weather-map-berlin
grafana_origin="${GRAFANA_ORIGIN:-http://localhost:3001}"
grafana_auth="${GRAFANA_AUTH:-wama-admin:wama-admin}"
druid_origin="${DRUID_ORIGIN:-http://localhost:8888}"
weather_wait_seconds="${WEATHER_MAP_INGEST_TIMEOUT_SECONDS:-420}"

cd "$repository_root"

show_failure_diagnostics() {
  printf '%s\n' "Berlin weather map validation failed; leaving the stack running for inspection."
  docker compose ps --all || true
  docker compose logs --tail 100 druid kafka grafana || true
  docker compose -f "$processor_repository/compose.yaml" logs --tail 100 || true
}

trap show_failure_diagnostics ERR

druid_sql() {
  curl --fail --silent --show-error \
    --max-time 90 \
    -X POST "$druid_origin/druid/v2/sql" \
    -H 'Content-Type: application/json' \
    --data "$1"
}

printf '%s\n' "1/5 processor unit tests"
docker build --target test --no-cache-filter test \
  -t wama.local/processor-weather-map:test "$processor_repository"

printf '%s\n' "2/5 processor deployed and publishing"
docker build -t wama.local/processor-weather-map:main "$processor_repository"
docker compose -f "$processor_repository/compose.yaml" up -d --no-build

printf '%s\n' "3/5 Open-Meteo weather reached Druid"
deadline=$((SECONDS + weather_wait_seconds))
while ((SECONDS < deadline)); do
  weather_mrid_count="$(druid_sql '{"query":"SELECT COUNT(DISTINCT \"mrid\") AS c FROM \"live_measurements\" WHERE \"mrid\" LIKE '"'"'urn:wama:poc:weather:berlin:%'"'"'"}' \
    | sed -n 's/.*"c":\([0-9]*\).*/\1/p')"
  if [[ "${weather_mrid_count:-0}" -ge 3 ]]; then
    break
  fi
  sleep 10
done
if [[ "${weather_mrid_count:-0}" -lt 3 ]]; then
  printf '%s\n' "Druid holds ${weather_mrid_count:-0} of 3 Berlin weather signals after ${weather_wait_seconds}s." >&2
  exit 1
fi
printf '%s\n' "    all 3 Berlin weather signals are queryable in Druid"

printf '%s\n' "4/5 PMU frequency signals present"
pmu_mrid_count="$(druid_sql '{"query":"SELECT COUNT(DISTINCT \"mrid\") AS c FROM \"live_measurements\" WHERE \"mrid\" LIKE '"'"'urn:wama:poc:pmu:bay-0%:frequency'"'"'"}' \
  | sed -n 's/.*"c":\([0-9]*\).*/\1/p')"
if [[ "${pmu_mrid_count:-0}" -lt 5 ]]; then
  printf '%s\n' "Druid holds ${pmu_mrid_count:-0} of 5 mapped PMU frequency signals." >&2
  exit 1
fi
printf '%s\n' "    all 5 mapped PMU frequency signals are queryable in Druid"

printf '%s\n' "5/5 both geomap panels return mappable rows through Grafana"
GRAFANA_ORIGIN="$grafana_origin" \
GRAFANA_AUTH="$grafana_auth" \
DASHBOARD_UID="$dashboard_uid" \
  python3 "$repository_root/services/grafana/tests/check_weather_map_panels.py"

trap - ERR
printf '%s\n' "Berlin weather map validation passed."
