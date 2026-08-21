#!/bin/sh

set -eu

repository_root="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
canonical_schema="$repository_root/docs/wama/schema/masterdata.proto"
canonical_rtd_schema="$repository_root/docs/wama/schema/rtd_schema.proto"
seed_root="$repository_root/forgejo-repos/gateway-c37-118-onboarding"

cmp "$canonical_schema" "$seed_root/contracts/masterdata.proto"
cmp "$canonical_rtd_schema" "$seed_root/contracts/rtd_schema.proto"
docker build --target test --file "$seed_root/Dockerfile" "$seed_root"
docker compose -f "$seed_root/compose.yaml" config --quiet
sh "$repository_root/services/forgejo-init/tests/test_bootstrap_processors.sh"