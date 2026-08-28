#!/bin/sh

set -eu

identity_file=/data/forgejo-gateway-c37-118-agent.identity
token_file=/data/forgejo-gateway-c37-118-agent.token
script_directory="$(CDPATH= cd -- "$(dirname "$0")" && pwd)"
infrastructure_root="$(CDPATH= cd -- "$script_directory/.." && pwd)"

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

usage() {
  printf '%s\n' "Usage: $0 [--checkout <gateway-c37-118 checkout>]" >&2
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
  '') ;;
  --checkout)
    [ "$#" -eq 2 ] || usage
    checkout="$2"
    ;;
  --help | -h) usage ;;
  *) usage ;;
esac

read_identity
if [ -z "$checkout" ]; then
  checkout="$infrastructure_root/forgejo-repos/$repository"
fi
[ -d "$checkout" ] || fail "The C37.118 gateway checkout does not exist: $checkout"
checkout="$(CDPATH= cd -- "$checkout" && pwd -P)"
if [ "$checkout" = "$infrastructure_root" ]; then
  fail "Refusing to configure the infrastructure checkout"
fi
git -C "$checkout" rev-parse --is-inside-work-tree >/dev/null 2>&1 \
  || fail "The C37.118 gateway checkout is not a Git worktree"
worktree_root="$(git -C "$checkout" rev-parse --show-toplevel)"
worktree_root="$(CDPATH= cd -- "$worktree_root" && pwd -P)"
[ "$checkout" = "$worktree_root" ] || fail "The C37.118 gateway checkout must be the Git worktree root"
origin="$(git -C "$checkout" remote get-url origin)" \
  || fail "The C37.118 gateway checkout must have an origin remote"
parse_origin "$origin"

token="$(docker compose --project-directory "$infrastructure_root" exec -T forgejo-runner cat "$token_file")" \
  || fail "Unable to read the Forgejo C37.118 gateway agent token from forgejo-runner"
[ -n "$token" ] || fail "Forgejo C37.118 gateway agent token is empty"

config_root="${XDG_CONFIG_HOME:-$HOME/.config}"
credential_directory="$config_root/wama-forgejo"
credential_file="$credential_directory/gateway-c37-118.credentials"
umask 077
mkdir -p "$credential_directory"
chmod 700 "$credential_directory"
temporary_credential_file="$(mktemp "$credential_directory/.gateway-c37-118.credentials.XXXXXX")"
trap 'rm -f "$temporary_credential_file"' EXIT HUP INT TERM
printf 'protocol=%s\nhost=%s\npath=%s\nusername=%s\npassword=%s\n\n' \
  "$protocol" "$host" "$path" "$username" "$token" \
  | git credential-store --file "$temporary_credential_file" store
chmod 600 "$temporary_credential_file"
mv -f "$temporary_credential_file" "$credential_file"
trap - EXIT HUP INT TERM

git -C "$checkout" config --local credential.useHttpPath true
git -C "$checkout" config --local credential.helper "store --file=$credential_file"

printf '%s\n' "Configured Forgejo credentials for $owner/$repository in $checkout."