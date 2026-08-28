#!/bin/sh

set -eu

repository_root="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
canonical_schema="$repository_root/docs/wama/schema/masterdata.proto"
canonical_rtd_schema="$repository_root/docs/wama/schema/rtd_schema.proto"
onboarding_checkout="$repository_root/forgejo-repos/gateway-c37-118-onboarding"

cmp "$canonical_schema" "$onboarding_checkout/contracts/masterdata.proto"
cmp "$canonical_rtd_schema" "$onboarding_checkout/contracts/rtd_schema.proto"
docker build --target test --file "$onboarding_checkout/Dockerfile" "$onboarding_checkout"
docker compose -f "$onboarding_checkout/compose.yaml" config --quiet
sh "$repository_root/services/forgejo-init/tests/test_bootstrap_processors.sh"
sh "$repository_root/services/forgejo-init/tests/test_gateway_onboarding_agent_credentials.sh"