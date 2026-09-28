#!/bin/sh

set -eu

deployment_root="${WAMA_GATEWAY_C37_118_DEPLOY_ROOT:-/var/lib/wama-gateway-c37-118}"
marker="$deployment_root/.wama-forgejo-gateway-c37-118-root"
manifest="$deployment_root/.wama-forgejo-gateway-c37-118-manifest.json"
overlay="$deployment_root/generated-adapters.compose.yaml"
project_name="${WAMA_GATEWAY_COMPOSE_PROJECT_NAME:-wama-gateway-c37-118}"

if [ ! -f "$marker" ] || [ ! -f "$manifest" ] || [ ! -f "$overlay" ]; then
  printf '%s\n' "Gateway deployment root is not prepared: $deployment_root" >&2
  exit 1
fi

revision="$(sed -n 's/^[[:space:]]*"commit":[[:space:]]*"\([^"]*\)".*/\1/p' "$manifest")"
image="$(sed -n 's/^[[:space:]]*"image":[[:space:]]*"\([^"]*\)".*/\1/p' "$manifest")"
if [ -z "$revision" ] || [ -z "$image" ]; then
  printf '%s\n' "Gateway deployment manifest has no immutable image revision." >&2
  exit 1
fi

compose() {
  docker compose --project-name "$project_name" \
    -f compose.yaml -f generated-adapters.compose.yaml "$@"
}

cd "$deployment_root"
export WAMA_CATALOG_REVISION="$revision"
export WAMA_GATEWAY_IMAGE="$image"
expected_services="masterdata-publisher"
for source in catalog/sources/*.yaml; do
  source_id="$(basename "$source" .yaml)"
  expected_services="$expected_services
c37-118-gateway-$source_id"
done
expected_services="$(printf '%s\n' "$expected_services" | sort)"
actual_services="$(compose config --services | sort)"
if [ "$actual_services" != "$expected_services" ]; then
  printf '%s\n' "Generated services do not match the reviewed catalog." >&2
  exit 1
fi

running_services="$(compose ps --status running --services | sort)"
expected_adapters="$(printf '%s\n' "$expected_services" | grep '^c37-118-gateway-')"
if [ "$running_services" != "$expected_adapters" ]; then
  printf '%s\n' "Not every catalog-derived C37.118 adapter is running." >&2
  exit 1
fi

compose run --rm --no-deps masterdata-publisher
compose run --rm --no-deps masterdata-publisher python -m gateway_c37_118.verify_masterdata
compose run --rm --no-deps masterdata-publisher python -m gateway_c37_118.verify_live_measurements
