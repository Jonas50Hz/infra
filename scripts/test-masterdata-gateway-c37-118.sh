#!/bin/sh

set -eu

repository_root="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
canonical_schema="$repository_root/docs/wama/schema/masterdata.proto"
canonical_rtd_schema="$repository_root/docs/wama/schema/rtd_schema.proto"
gateway_checkout="$repository_root/forgejo-repos/gateway-c37-118"

cmp "$canonical_schema" "$gateway_checkout/contracts/masterdata.proto"
cmp "$canonical_rtd_schema" "$gateway_checkout/contracts/rtd_schema.proto"
docker build --target test --file "$gateway_checkout/Dockerfile" "$gateway_checkout"
docker compose -f "$gateway_checkout/compose.yaml" config --quiet
sh "$repository_root/services/forgejo-init/tests/test_bootstrap_processors.sh"
sh "$repository_root/services/forgejo-init/tests/test_gateway_c37_118_agent_credentials.sh"