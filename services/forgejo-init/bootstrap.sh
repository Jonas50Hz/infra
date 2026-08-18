#!/bin/sh

set -eu

server_config="${FORGEJO_SERVER_CONFIG:-/data/gitea/conf/app.ini}"
runner_dir="${FORGEJO_RUNNER_DIRECTORY:-/runner}"
runner_config_file="$runner_dir/config.yaml"
runner_scope_file="$runner_dir/forgejo-processors.scope"
runner_layout_file="$runner_dir/forgejo-processors.layout"
runner_layout=two-connections-v1
runner_ci_secret_file="$runner_dir/forgejo-processors-ci.secret"
runner_ci_uuid_file="$runner_dir/forgejo-processors-ci.uuid"
runner_deploy_secret_file="$runner_dir/forgejo-processors-deploy.secret"
runner_deploy_uuid_file="$runner_dir/forgejo-processors-deploy.uuid"
runner_package_token_file="$runner_dir/forgejo-processors-package.token"
package_token_name=wama-processors-package-publish
seed_directory="${FORGEJO_PROCESSORS_SEED_DIRECTORY:-/opt/wama/seed}"
admin_username="${FORGEJO_BOOTSTRAP_ADMIN_USERNAME:-wama-admin}"
admin_email="${FORGEJO_BOOTSTRAP_ADMIN_EMAIL:-wama-admin@local}"
admin_password="${FORGEJO_BOOTSTRAP_ADMIN_PASSWORD:-wama-admin}"
repository="${FORGEJO_PROCESSORS_REPOSITORY:-wama-processors}"
runner_url="${FORGEJO_RUNNER_URL:-}"
compose_project_name="${WAMA_PROCESSORS_COMPOSE_PROJECT_NAME:-wama-processors}"
deploy_root="${WAMA_PROCESSORS_DEPLOY_ROOT:-}"
infra_network="${WAMA_INFRA_NETWORK:-wama-infra}"
runner_ci_name=wama-processors-ci
runner_deploy_name=wama-processors-deploy
runner_ci_label=wama-processors-ci:docker://wama-forgejo-runner:local
runner_deploy_label=wama-processors-deploy:host
runner_scope="$admin_username/$repository"
api_url="${FORGEJO_API_URL:-http://forgejo:3000/api/v1}"

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

initialize_deploy_root() {
  deploy_marker="$deploy_root/.wama-forgejo-processors-root"
  if [ -e "$deploy_root" ] && [ ! -d "$deploy_root" ]; then
    printf '%s\n' "WAMA_PROCESSORS_DEPLOY_ROOT must be a directory" >&2
    exit 1
  fi
  mkdir -p "$deploy_root"

  if [ -e "$deploy_marker" ]; then
    if [ -L "$deploy_marker" ] || [ ! -f "$deploy_marker" ]; then
      printf '%s\n' "WAMA_PROCESSORS_DEPLOY_ROOT has an invalid deployment marker" >&2
      exit 1
    fi
    return
  fi

  if [ -n "$(find "$deploy_root" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    printf '%s\n' "WAMA_PROCESSORS_DEPLOY_ROOT must be empty before bootstrap creates its marker" >&2
    exit 1
  fi
  printf '%s\n' "Managed by the WAMA Forgejo processors repository." > "$deploy_marker"
}

repository_is_private() {
  grep -Eq '"private"[[:space:]]*:[[:space:]]*true' "$1"
}

ensure_repository() {
  repository_response="$temporary_directory/repository.json"
  if wget -q -O "$repository_response" \
    --header="Authorization: Basic $api_auth_header" \
    "$api_url/repos/$runner_scope" 2>/dev/null; then
    if ! repository_is_private "$repository_response"; then
      printf '%s\n' "Existing Forgejo processors repository must be private" >&2
      exit 1
    fi
    return
  fi

  wget -q -O "$repository_response" \
    --header="Authorization: Basic $api_auth_header" \
    --header="Content-Type: application/json" \
    --post-data="{\"name\":\"$repository\",\"private\":true,\"auto_init\":false}" \
    "$api_url/user/repos"
  if ! repository_is_private "$repository_response"; then
    printf '%s\n' "Forgejo did not create a private processors repository" >&2
    exit 1
  fi
}

seed_repository_if_empty() {
  repository_url="http://forgejo:3000/$runner_scope.git"
  if ! repository_refs="$(GIT_TERMINAL_PROMPT=0 git \
    -c "http.extraHeader=Authorization: Basic $api_auth_header" \
    ls-remote "$repository_url")"; then
    printf '%s\n' "Unable to inspect Forgejo processors repository refs" >&2
    exit 1
  fi
  if [ -n "$repository_refs" ]; then
    printf '%s\n' "Forgejo processors repository already has refs; leaving it unchanged."
    return
  fi

  if [ ! -d "$seed_directory" ]; then
    printf '%s\n' "Forgejo processors seed is unavailable at $seed_directory" >&2
    exit 1
  fi

  seed_worktree="$temporary_directory/seed"
  mkdir "$seed_worktree"
  cp -R "$seed_directory"/. "$seed_worktree"
  git -C "$seed_worktree" init --initial-branch=main --quiet
  git -C "$seed_worktree" config user.name "WAMA Forgejo bootstrap"
  git -C "$seed_worktree" config user.email "$admin_email"
  git -C "$seed_worktree" add --all
  git -C "$seed_worktree" commit --quiet -m "Seed WAMA processors"
  GIT_TERMINAL_PROMPT=0 git -C "$seed_worktree" \
    -c "http.extraHeader=Authorization: Basic $api_auth_header" \
    push "$repository_url" HEAD:refs/heads/main
}

register_runner() {
  runner_name="$1"
  runner_label="$2"
  runner_secret_file="$3"
  runner_uuid_file="$4"

  if [ ! -s "$runner_secret_file" ]; then
    forgejo_as_git forgejo-cli actions generate-secret > "$runner_secret_file"
  fi
  chown git:git "$runner_secret_file"

  runner_uuid="$(forgejo_as_git forgejo-cli actions register \
    --name "$runner_name" \
    --secret-file "$runner_secret_file" \
    --scope "$runner_scope" \
    --labels "$runner_label" | tr -d '\r\n')"
  if [ -n "$runner_uuid" ]; then
    printf '%s\n' "$runner_uuid" > "$runner_uuid_file"
  fi
  if [ ! -s "$runner_uuid_file" ]; then
    printf '%s\n' "Forgejo did not return a runner UUID for $runner_name" >&2
    exit 1
  fi
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

reset_runner_state() {
  rm -f \
    "$runner_dir/forgejo-runner.secret" \
    "$runner_dir/forgejo-runner.uuid" \
    "$runner_dir/forgejo-runner.scope" \
    "$runner_ci_secret_file" \
    "$runner_ci_uuid_file" \
    "$runner_deploy_secret_file" \
    "$runner_deploy_uuid_file" \
    "$runner_config_file" \
    "$runner_dir/.runner"
}

if [ ! -f "$server_config" ]; then
  printf '%s\n' "Forgejo configuration is unavailable at $server_config" >&2
  exit 1
fi

require_value FORGEJO_BOOTSTRAP_ADMIN_PASSWORD "$admin_password"
require_value FORGEJO_RUNNER_URL "$runner_url"
require_value WAMA_PROCESSORS_DEPLOY_ROOT "$deploy_root"
validate_identifier FORGEJO_BOOTSTRAP_ADMIN_USERNAME "$admin_username"
validate_identifier FORGEJO_PROCESSORS_REPOSITORY "$repository"
case "$deploy_root" in
  /) printf '%s\n' "WAMA_PROCESSORS_DEPLOY_ROOT must not be /" >&2; exit 1 ;;
  /*) ;;
  *) printf '%s\n' "WAMA_PROCESSORS_DEPLOY_ROOT must be an absolute path" >&2; exit 1 ;;
esac

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
initialize_deploy_root

api_auth_header="$(printf '%s:%s' "$admin_username" "$admin_password" | base64 | tr -d '\n')"
ensure_repository
seed_repository_if_empty
ensure_package_token

if [ ! -f "$runner_layout_file" ] || [ "$(cat "$runner_layout_file")" != "$runner_layout" ]; then
  reset_runner_state
fi

if [ -f "$runner_scope_file" ] && [ "$(cat "$runner_scope_file")" != "$runner_scope" ]; then
  reset_runner_state
fi

register_runner "$runner_ci_name" "$runner_ci_label" "$runner_ci_secret_file" "$runner_ci_uuid_file"
register_runner "$runner_deploy_name" "$runner_deploy_label" "$runner_deploy_secret_file" "$runner_deploy_uuid_file"
runner_ci_uuid="$(cat "$runner_ci_uuid_file")"
runner_ci_secret="$(cat "$runner_ci_secret_file")"
runner_deploy_uuid="$(cat "$runner_deploy_uuid_file")"
runner_deploy_secret="$(cat "$runner_deploy_secret_file")"
runner_package_token="$(cat "$runner_package_token_file")"
printf '%s\n' "$runner_scope" > "$runner_scope_file"
printf '%s\n' "$runner_layout" > "$runner_layout_file"

cat > "$runner_config_file" <<EOF
log:
  level: info
  job_level: info
runner:
  file: /data/.runner
  capacity: 1
  labels:
    - "$runner_ci_label"
    - "$runner_deploy_label"
  envs:
    WAMA_PROCESSORS_DEPLOY_ROOT: "$deploy_root"
    WAMA_PROCESSORS_COMPOSE_PROJECT_NAME: "$compose_project_name"
    WAMA_INFRA_NETWORK: "$infra_network"
    FORGEJO_PROCESSORS_PACKAGE_USERNAME: "$admin_username"
    FORGEJO_PROCESSORS_PACKAGE_TOKEN: "$runner_package_token"
container:
  docker_host: automount
  valid_volumes:
    - "$deploy_root"
  options: "--add-host=host.docker.internal:host-gateway --cpus=2 --memory=2g"
server:
  connections:
    $runner_ci_name:
      url: $runner_url
      uuid: $runner_ci_uuid
      token: $runner_ci_secret
    $runner_deploy_name:
      url: $runner_url
      uuid: $runner_deploy_uuid
      token: $runner_deploy_secret
EOF