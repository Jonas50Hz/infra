#!/bin/sh

set -eu

infrastructure_root="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"

if ! docker compose --project-directory "$infrastructure_root" ps --status running --quiet forgejo | grep -q .; then
  printf '%s\n' "Forgejo must be running before processor administration." >&2
  exit 1
fi

exec docker compose --project-directory "$infrastructure_root" run --rm --no-deps \
  --entrypoint python3 forgejo-init /opt/wama/processor_registry.py "$@"