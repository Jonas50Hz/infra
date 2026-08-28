#!/bin/sh

set -eu

identity_file=/data/forgejo-gateway-c37-118-agent.identity
script_directory="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"
infrastructure_root="$(CDPATH= cd -- "$script_directory/.." && pwd)"

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

usage() {
  printf '%s\n' "Usage: $0 [--checkout <gateway-c37-118 checkout>] -- <command> [args...]" >&2
  exit 2
}

validate_identifier() {
  value="$1"
  case "$value" in
    '' | *[!A-Za-z0-9._-]*) return 1 ;;
  esac
}

read_identity() {
  identity="$(docker compose --project-directory "$infrastructure_root" exec -T forgejo-runner cat "$identity_file")" \
    || fail "Unable to read the Forgejo C37.118 gateway agent identity from forgejo-runner"
  owner=
  repository=
  username=
  while IFS='=' read -r key value; do
    case "$key" in
      owner) owner="$value" ;;
      repository) repository="$value" ;;
      username) username="$value" ;;
      *) fail "Forgejo C37.118 gateway agent identity contains an unknown field" ;;
    esac
  done <<EOF
$identity
EOF
  validate_identifier "$owner" || fail "Forgejo C37.118 gateway agent identity has an invalid owner"
  validate_identifier "$repository" || fail "Forgejo C37.118 gateway agent identity has an invalid repository"
  validate_identifier "$username" || fail "Forgejo C37.118 gateway agent identity has an invalid username"
}

parse_origin() {
  origin="$1"
  expected_suffix="/$owner/$repository.git"
  case "$origin" in
    http://* | https://*) ;;
    *) fail "The C37.118 gateway checkout origin must use HTTP or HTTPS" ;;
  esac
  case "$origin" in
    *"$expected_suffix") ;;
    *) fail "The C37.118 gateway checkout origin is not $owner/$repository" ;;
  esac
  base_url="${origin%"$expected_suffix"}"
  remainder="${base_url#*://}"
  case "$remainder" in
    '' | *@* | *\?* | *\#*) fail "The C37.118 gateway checkout origin must not contain credentials, a query, or a fragment" ;;
  esac
  protocol="${base_url%%://*}"
  host="${remainder%%/*}"
  if [ "$host" = "$remainder" ]; then
    path="$expected_suffix"
  else
    path="/${remainder#*/}$expected_suffix"
  fi
  path="${path#/}"
  case "$host" in
    '') fail "The C37.118 gateway checkout origin is missing a host" ;;
  esac
}

checkout=
case "${1:-}" in
  --checkout)
    [ "$#" -ge 4 ] || usage
    checkout="$2"
    shift 2
    ;;
  --) ;;
  *) usage ;;
esac
shift
[ "$#" -gt 0 ] || usage

read_identity
if [ -z "$checkout" ]; then
  checkout="$infrastructure_root/forgejo-repos/$repository"
fi
[ -d "$checkout" ] || fail "The C37.118 gateway checkout does not exist: $checkout"
checkout="$(CDPATH= cd -- "$checkout" && pwd -P)"
if [ "$checkout" = "$infrastructure_root" ]; then
  fail "Refusing to use the infrastructure checkout"
fi
git -C "$checkout" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
  || fail "The C37.118 gateway checkout is not a Git worktree"
worktree_root="$(git -C "$checkout" rev-parse --show-toplevel)"
worktree_root="$(CDPATH= cd -- "$worktree_root" && pwd -P)"
[ "$checkout" = "$worktree_root" ] || fail "The C37.118 gateway checkout must be the Git worktree root"
origin="$(git -C "$checkout" remote get-url origin)" \
  || fail "The C37.118 gateway checkout must have an origin remote"
parse_origin "$origin"

credential="$(printf 'protocol=%s\nhost=%s\npath=%s\n\n' "$protocol" "$host" "$path" \
  | GIT_CONFIG_NOSYSTEM=1 GIT_CONFIG_GLOBAL=/dev/null git -C "$checkout" credential fill)"
agent_username=
agent_token=
while IFS='=' read -r key value; do
  case "$key" in
    username) agent_username="$value" ;;
    password) agent_token="$value" ;;
  esac
done <<EOF
$credential
EOF
validate_identifier "$agent_username" || fail "The C37.118 gateway checkout does not have a valid Forgejo agent credential"
[ "$agent_username" = "$username" ] \
  || fail "The C37.118 gateway checkout Forgejo credential does not match its configured agent"
[ -n "$agent_token" ] || fail "The C37.118 gateway checkout does not have a Forgejo agent token"

FORGEJO_ROOT_URL="$base_url" \
FORGEJO_API_URL="${base_url%/}/api/v1" \
FORGEJO_OWNER="$owner" \
FORGEJO_REPOSITORY="$repository" \
FORGEJO_AGENT_USERNAME="$agent_username" \
FORGEJO_API_TOKEN="$agent_token" \
exec "$@"