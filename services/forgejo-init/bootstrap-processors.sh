#!/bin/sh

set -eu

server_config="${FORGEJO_SERVER_CONFIG:-/data/gitea/conf/app.ini}"
runner_dir="${FORGEJO_RUNNER_DIRECTORY:-/runner}"
runner_package_token_file="$runner_dir/forgejo-processors-package.token"
package_token_name=wama-processors-package-publish
seed_root="${FORGEJO_PROCESSOR_SEED_ROOT:-/opt/wama/seeds}"
admin_username="${FORGEJO_BOOTSTRAP_ADMIN_USERNAME:-wama-admin}"
admin_email="${FORGEJO_BOOTSTRAP_ADMIN_EMAIL:-wama-admin@local}"
admin_password="${FORGEJO_BOOTSTRAP_ADMIN_PASSWORD:-wama-admin}"
api_url="${FORGEJO_API_URL:-http://forgejo:3000/api/v1}"
runner_url="${FORGEJO_RUNNER_URL:-}"
processor_deploy_base_root="${WAMA_PROCESSOR_DEPLOY_BASE_ROOT:-/var/lib/wama-processors}"
processor_registry_script="${WAMA_PROCESSOR_REGISTRY_SCRIPT:-/opt/wama/processor_registry.py}"
gateway_repository="${FORGEJO_GATEWAY_C37_118_ONBOARDING_REPOSITORY:-gateway-c37-118-onboarding}"
gateway_deploy_root="${WAMA_GATEWAY_C37_118_ONBOARDING_DEPLOY_ROOT:-/var/lib/wama-gateway-c37-118-onboarding}"
gateway_marker=.wama-forgejo-gateway-onboarding-root
gateway_agent_username="${FORGEJO_GATEWAY_C37_118_ONBOARDING_AGENT_USERNAME:-wama-gateway-c37-118-onboarding-agent}"
gateway_agent_email="${FORGEJO_GATEWAY_C37_118_ONBOARDING_AGENT_EMAIL:-wama-gateway-c37-118-onboarding-agent@local}"
gateway_agent_token_name=wama-gateway-c37-118-onboarding-agent
gateway_agent_token_file="$runner_dir/forgejo-gateway-c37-118-onboarding-agent.token"
gateway_agent_identity_file="$runner_dir/forgejo-gateway-c37-118-onboarding-agent.identity"
runner_ci_label=wama-processors-ci:docker://wama-forgejo-runner:local
runner_deploy_label=wama-processors-deploy:host

forgejo_as_git() {
  s6-setuidgid git forgejo --config "$server_config" "$@"
}

require_value() {
  name="$1"
  value="$2"
  if [ -z "$value" ]; then
    printf '%s\n' "$name must be configured before Forgejo bootstrap can run" >&2
    exit 1
  fi
}

validate_identifier() {
  name="$1"
  value="$2"
  case "$value" in
    '' | *[!A-Za-z0-9._-]*)
      printf '%s\n' "$name must contain only letters, numbers, dots, underscores, or hyphens" >&2
      exit 1
      ;;
  esac
}

validate_deploy_root() {
  name="$1"
  deploy_root="$2"
  case "$deploy_root" in
    /) printf '%s\n' "$name must not be /" >&2; exit 1 ;;
    /*) ;;
    *) printf '%s\n' "$name must be an absolute path" >&2; exit 1 ;;
  esac
}

initialize_gateway_root() {
  if [ -e "$gateway_deploy_root" ] && [ ! -d "$gateway_deploy_root" ]; then
    printf '%s\n' "$gateway_deploy_root must be a directory" >&2
    exit 1
  fi
  mkdir -p "$gateway_deploy_root"
  marker="$gateway_deploy_root/$gateway_marker"
  if [ -e "$marker" ]; then
    if [ -L "$marker" ] || [ ! -f "$marker" ]; then
      printf '%s\n' "$gateway_deploy_root has an invalid deployment marker" >&2
      exit 1
    fi
    return
  fi
  if [ -n "$(find "$gateway_deploy_root" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    printf '%s\n' "$gateway_deploy_root must be empty before bootstrap creates its marker" >&2
    exit 1
  fi
  printf '%s\n' "Managed by the WAMA Forgejo repository $gateway_repository." > "$marker"
}

repository_is_private() {
  grep -Eq '"private"[[:space:]]*:[[:space:]]*true' "$1"
}

ensure_repository() {
  repository="$1"
  scope="$admin_username/$repository"
  response="$temporary_directory/$repository.json"
  if wget -q -O "$response" --header="Authorization: Basic $api_auth_header" \
    "$api_url/repos/$scope" 2>/dev/null; then
    if ! repository_is_private "$response"; then
      printf '%s\n' "Existing Forgejo repository $repository must be private" >&2
      exit 1
    fi
    return
  fi
  wget -q -O "$response" \
    --header="Authorization: Basic $api_auth_header" \
    --header="Content-Type: application/json" \
    --post-data="{\"name\":\"$repository\",\"private\":true,\"auto_init\":false}" \
    "$api_url/user/repos"
  if ! repository_is_private "$response"; then
    printf '%s\n' "Forgejo did not create a private repository $repository" >&2
    exit 1
  fi
}

seed_repository_if_empty() {
  repository="$1"
  seed_directory="$2"
  repository_url="${FORGEJO_INTERNAL_ROOT_URL:-http://forgejo:3000}/$admin_username/$repository.git"
  if ! refs="$(GIT_TERMINAL_PROMPT=0 git \
    -c "http.extraHeader=Authorization: Basic $api_auth_header" \
    ls-remote "$repository_url")"; then
    printf '%s\n' "Unable to inspect Forgejo repository $repository refs" >&2
    exit 1
  fi
  if [ -n "$refs" ]; then
    printf '%s\n' "Forgejo repository $repository already has refs; leaving it unchanged."
    return
  fi
  if [ ! -d "$seed_directory" ]; then
    printf '%s\n' "Forgejo seed is unavailable for $repository: $seed_directory" >&2
    exit 1
  fi
  seed_worktree="$temporary_directory/seed-$repository"
  mkdir "$seed_worktree"
  cp -R "$seed_directory"/. "$seed_worktree"
  git -C "$seed_worktree" init --initial-branch=main --quiet
  git -C "$seed_worktree" config user.name "WAMA Forgejo bootstrap"
  git -C "$seed_worktree" config user.email "$admin_email"
  git -C "$seed_worktree" add --all
  git -C "$seed_worktree" commit --quiet -m "Seed $repository"
  GIT_TERMINAL_PROMPT=0 git -C "$seed_worktree" \
    -c "http.extraHeader=Authorization: Basic $api_auth_header" \
    push "$repository_url" HEAD:refs/heads/main
}

ensure_package_token() {
  if [ ! -s "$runner_package_token_file" ]; then
    forgejo_as_git admin user generate-access-token \
      --username "$admin_username" \
      --token-name "$package_token_name" \
      --scopes write:package \
      --raw > "$runner_package_token_file"
  fi
  chown git:git "$runner_package_token_file"
  if [ ! -s "$runner_package_token_file" ]; then
    printf '%s\n' "Forgejo did not generate a processors package token" >&2
    exit 1
  fi
}

forgejo_api_as_admin() {
  curl --fail --silent --show-error \
    --user "$admin_username:$admin_password" \
    "$@"
}

lookup_gateway_agent() {
  gateway_agent_users_file="$temporary_directory/gateway-agent-users.json"
  gateway_agent_file="$temporary_directory/gateway-agent.json"
  forgejo_api_as_admin \
    --output "$gateway_agent_users_file" \
    "$api_url/admin/users?limit=100"
  if ! jq -e --arg username "$gateway_agent_username" \
    '.[] | select(.username == $username)' \
    "$gateway_agent_users_file" > "$gateway_agent_file"; then
    : > "$gateway_agent_file"
  fi
}

gateway_agent_is_restricted_non_admin() {
  [ -s "$gateway_agent_file" ] \
    && jq -e --arg username "$gateway_agent_username" \
      '.username == $username and .is_admin == false and .restricted == true' \
      "$gateway_agent_file" > /dev/null
}

ensure_gateway_agent_user() {
  lookup_gateway_agent
  if [ ! -s "$gateway_agent_file" ]; then
    gateway_agent_password="$(dd if=/dev/urandom bs=48 count=1 2>/dev/null | base64 | tr -d '\n')"
    if [ -z "$gateway_agent_password" ]; then
      printf '%s\n' "Unable to generate a Forgejo gateway onboarding agent password" >&2
      exit 1
    fi
    forgejo_as_git admin user create \
      --username "$gateway_agent_username" \
      --password "$gateway_agent_password" \
      --email "$gateway_agent_email" \
      --restricted \
      --must-change-password=false
    lookup_gateway_agent
  fi
  if ! gateway_agent_is_restricted_non_admin; then
    printf '%s\n' "Forgejo gateway onboarding agent must be a restricted non-admin user" >&2
    exit 1
  fi
}

ensure_gateway_agent_collaboration() {
  forgejo_api_as_admin \
    --request PUT \
    --header "Content-Type: application/json" \
    --data '{"permission":"write"}' \
    --output "$temporary_directory/gateway-agent-collaboration.json" \
    "$api_url/repos/$admin_username/$gateway_repository/collaborators/$gateway_agent_username"
}

gateway_agent_token_is_valid() {
  if [ ! -s "$gateway_agent_token_file" ]; then
    return 1
  fi
  gateway_agent_token="$(cat "$gateway_agent_token_file")"
  if [ -z "$gateway_agent_token" ]; then
    return 1
  fi
  gateway_agent_token_response="$temporary_directory/gateway-agent-token.json"
  if ! curl --fail --silent --show-error \
    --header "Authorization: token $gateway_agent_token" \
    --output "$gateway_agent_token_response" \
    "$api_url/user"; then
    return 1
  fi
  grep -Fq "\"username\":\"$gateway_agent_username\"" "$gateway_agent_token_response"
}

ensure_gateway_agent_token() {
  if ! gateway_agent_token_is_valid; then
    rm -f "$gateway_agent_token_file"
    forgejo_as_git admin user generate-access-token \
      --username "$gateway_agent_username" \
      --token-name "$gateway_agent_token_name" \
      --scopes all \
      --raw > "$gateway_agent_token_file"
  fi
  chown git:git "$gateway_agent_token_file"
  chmod 600 "$gateway_agent_token_file"
  if ! gateway_agent_token_is_valid; then
    printf '%s\n' "Forgejo gateway onboarding agent token is invalid" >&2
    exit 1
  fi
}

write_gateway_agent_identity() {
  cat > "$gateway_agent_identity_file" <<EOF
owner=$admin_username
repository=$gateway_repository
username=$gateway_agent_username
EOF
  chown git:git "$gateway_agent_identity_file"
  chmod 600 "$gateway_agent_identity_file"
}

register_gateway_runner() {
  runner_name="$1"
  runner_label="$2"
  secret_file="$runner_dir/$runner_name.secret"
  uuid_file="$runner_dir/$runner_name.uuid"
  if [ ! -s "$secret_file" ]; then
    forgejo_as_git forgejo-cli actions generate-secret > "$secret_file"
  fi
  chown git:git "$secret_file"
  uuid="$(forgejo_as_git forgejo-cli actions register \
    --name "$runner_name" \
    --secret-file "$secret_file" \
    --scope "$admin_username/$gateway_repository" \
    --labels "$runner_label" | tr -d '\r\n')"
  if [ -n "$uuid" ]; then
    printf '%s\n' "$uuid" > "$uuid_file"
  fi
  if [ ! -s "$uuid_file" ]; then
    printf '%s\n' "Forgejo did not return a runner UUID for $runner_name" >&2
    exit 1
  fi
}

if [ ! -f "$server_config" ]; then
  printf '%s\n' "Forgejo configuration is unavailable at $server_config" >&2
  exit 1
fi

require_value FORGEJO_BOOTSTRAP_ADMIN_PASSWORD "$admin_password"
require_value FORGEJO_RUNNER_URL "$runner_url"
require_value FORGEJO_GATEWAY_C37_118_ONBOARDING_AGENT_EMAIL "$gateway_agent_email"
require_value WAMA_PROCESSOR_DEPLOY_BASE_ROOT "$processor_deploy_base_root"
require_value WAMA_GATEWAY_C37_118_ONBOARDING_DEPLOY_ROOT "$gateway_deploy_root"
validate_identifier FORGEJO_BOOTSTRAP_ADMIN_USERNAME "$admin_username"
validate_identifier FORGEJO_GATEWAY_C37_118_ONBOARDING_REPOSITORY "$gateway_repository"
validate_identifier FORGEJO_GATEWAY_C37_118_ONBOARDING_AGENT_USERNAME "$gateway_agent_username"
validate_deploy_root WAMA_PROCESSOR_DEPLOY_BASE_ROOT "$processor_deploy_base_root"
validate_deploy_root WAMA_GATEWAY_C37_118_ONBOARDING_DEPLOY_ROOT "$gateway_deploy_root"

if ! forgejo_as_git admin user list | awk -v username="$admin_username" '$2 == username { found = 1 } END { exit !found }'; then
  forgejo_as_git admin user create \
    --username "$admin_username" \
    --password "$admin_password" \
    --email "$admin_email" \
    --admin \
    --must-change-password=false
fi

umask 077
mkdir -p "$runner_dir"
temporary_directory="$(mktemp -d)"
trap 'rm -rf "$temporary_directory"' EXIT HUP INT TERM
initialize_gateway_root
api_auth_header="$(printf '%s:%s' "$admin_username" "$admin_password" | base64 | tr -d '\n')"
ensure_repository "$gateway_repository"
seed_repository_if_empty "$gateway_repository" "$seed_root/gateway-c37-118-onboarding"
ensure_package_token
ensure_gateway_agent_user
ensure_gateway_agent_collaboration
ensure_gateway_agent_token
write_gateway_agent_identity
register_gateway_runner "wama-$gateway_repository-ci" "$runner_ci_label"
register_gateway_runner "wama-$gateway_repository-deploy" "$runner_deploy_label"

if [ ! -f "$processor_registry_script" ]; then
  printf '%s\n' "Processor registry script is unavailable: $processor_registry_script" >&2
  exit 1
fi
python3 "$processor_registry_script" bootstrap