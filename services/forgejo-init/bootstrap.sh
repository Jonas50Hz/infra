#!/bin/sh

set -eu

server_config=/data/gitea/conf/app.ini
runner_dir=/runner
runner_secret_file="$runner_dir/forgejo-runner.secret"
runner_uuid_file="$runner_dir/forgejo-runner.uuid"
runner_config_file="$runner_dir/config.yaml"
runner_scope_file="$runner_dir/forgejo-runner.scope"
admin_username="${FORGEJO_BOOTSTRAP_ADMIN_USERNAME:-wama-admin}"
admin_email="${FORGEJO_BOOTSTRAP_ADMIN_EMAIL:-wama-admin@local}"
admin_password="${FORGEJO_BOOTSTRAP_ADMIN_PASSWORD:-wama-admin}"
repository="${FORGEJO_APPLICATION_REPOSITORY:-wama-applications}"
runner_url="${FORGEJO_RUNNER_URL:-}"
compose_project_name="${WAMA_APPS_COMPOSE_PROJECT_NAME:-wama-applications}"
deploy_root="${WAMA_APPS_DEPLOY_ROOT:-}"
infra_network="${WAMA_INFRA_NETWORK:-wama-infra}"
runner_name=wama-applications
runner_ci_label=wama-app-ci:docker://wama-forgejo-runner:local
runner_deploy_label=wama-app-deploy:host
runner_labels="$runner_ci_label,$runner_deploy_label"
runner_scope="$admin_username/$repository"
api_url=http://forgejo:3000/api/v1

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

ensure_repository() {
  if wget -q -O /dev/null \
    --header="Authorization: Basic $api_auth_header" \
    "$api_url/repos/$runner_scope" 2>/dev/null; then
    return
  fi

  wget -q -O /dev/null \
    --header="Authorization: Basic $api_auth_header" \
    --header="Content-Type: application/json" \
    --post-data="{\"name\":\"$repository\",\"private\":true,\"auto_init\":false}" \
    "$api_url/user/repos"
}

if [ ! -f "$server_config" ]; then
  printf '%s\n' "Forgejo configuration is unavailable at $server_config" >&2
  exit 1
fi

require_value FORGEJO_BOOTSTRAP_ADMIN_PASSWORD "$admin_password"
require_value FORGEJO_RUNNER_URL "$runner_url"
require_value WAMA_APPS_DEPLOY_ROOT "$deploy_root"
validate_identifier FORGEJO_BOOTSTRAP_ADMIN_USERNAME "$admin_username"
validate_identifier FORGEJO_APPLICATION_REPOSITORY "$repository"
case "$deploy_root" in
  /) printf '%s\n' "WAMA_APPS_DEPLOY_ROOT must not be /" >&2; exit 1 ;;
  /*) ;;
  *) printf '%s\n' "WAMA_APPS_DEPLOY_ROOT must be an absolute path" >&2; exit 1 ;;
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
rm -f "$runner_dir/forgejo-bootstrap.token"
mkdir -p "$deploy_root"
deploy_marker="$deploy_root/.wama-forgejo-applications-root"
if [ ! -e "$deploy_marker" ]; then
  printf '%s\n' "Managed by the WAMA Forgejo application repository." > "$deploy_marker"
fi

api_auth_header="$(printf '%s:%s' "$admin_username" "$admin_password" | base64 | tr -d '\n')"
ensure_repository

if [ -f "$runner_scope_file" ] && [ "$(cat "$runner_scope_file")" != "$runner_scope" ]; then
  rm -f "$runner_secret_file" "$runner_uuid_file" "$runner_config_file" "$runner_dir/.runner"
fi

if [ ! -s "$runner_secret_file" ]; then
  forgejo_as_git forgejo-cli actions generate-secret > "$runner_secret_file"
fi
chown git:git "$runner_secret_file"

runner_uuid="$(forgejo_as_git forgejo-cli actions register \
  --name "$runner_name" \
  --secret-file "$runner_secret_file" \
  --scope "$runner_scope" \
  --labels "$runner_labels" | tr -d '\r\n')"

if [ -n "$runner_uuid" ]; then
  printf '%s\n' "$runner_uuid" > "$runner_uuid_file"
fi

if [ ! -s "$runner_uuid_file" ]; then
  printf '%s\n' "Forgejo did not return a runner UUID" >&2
  exit 1
fi

runner_uuid="$(cat "$runner_uuid_file")"
runner_secret="$(cat "$runner_secret_file")"
printf '%s\n' "$runner_scope" > "$runner_scope_file"

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
    WAMA_APPS_DEPLOY_ROOT: "$deploy_root"
    WAMA_APPS_COMPOSE_PROJECT_NAME: "$compose_project_name"
    WAMA_INFRA_NETWORK: "$infra_network"
container:
  docker_host: automount
  valid_volumes:
    - "$deploy_root"
  options: "--add-host=host.docker.internal:host-gateway --cpus=2 --memory=2g"
server:
  connections:
    forgejo:
      url: $runner_url
      uuid: $runner_uuid
      token: $runner_secret
EOF