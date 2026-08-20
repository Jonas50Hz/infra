#!/bin/sh

set -eu

server_config="${FORGEJO_SERVER_CONFIG:-/data/gitea/conf/app.ini}"
runner_dir="${FORGEJO_RUNNER_DIRECTORY:-/runner}"
runner_config_file="$runner_dir/config.yaml"
runner_layout_file="$runner_dir/forgejo-processor-repositories.layout"
runner_scope_file="$runner_dir/forgejo-processor-repositories.scope"
runner_layout=eight-connections-v4
runner_package_token_file="$runner_dir/forgejo-processors-package.token"
package_token_name=wama-processors-package-publish
seed_root="${FORGEJO_PROCESSOR_SEED_ROOT:-/opt/wama/seeds}"
admin_username="${FORGEJO_BOOTSTRAP_ADMIN_USERNAME:-wama-admin}"
admin_email="${FORGEJO_BOOTSTRAP_ADMIN_EMAIL:-wama-admin@local}"
admin_password="${FORGEJO_BOOTSTRAP_ADMIN_PASSWORD:-wama-admin}"
frequency_repository="${FORGEJO_FREQUENCY_SCALE_REPOSITORY:-processor-frequency-scale}"
apparent_repository="${FORGEJO_APPARENT_POWER_REPOSITORY:-processor-apparent-power}"
frequency_iec104_export_repository="${FORGEJO_FREQUENCY_IEC104_EXPORT_REPOSITORY:-processor-frequency-iec104-export}"
lfr_frequency_provision_repository="${FORGEJO_LFR_FREQUENCY_PROVISION_REPOSITORY:-processor-lfr-frequency-provision}"
frequency_deploy_root="${WAMA_FREQUENCY_SCALE_DEPLOY_ROOT:-/var/lib/wama-processor-frequency-scale}"
apparent_deploy_root="${WAMA_APPARENT_POWER_DEPLOY_ROOT:-/var/lib/wama-processor-apparent-power}"
frequency_iec104_export_deploy_root="${WAMA_FREQUENCY_IEC104_EXPORT_DEPLOY_ROOT:-/var/lib/wama-processor-frequency-iec104-export}"
lfr_frequency_provision_deploy_root="${WAMA_LFR_FREQUENCY_PROVISION_DEPLOY_ROOT:-/var/lib/wama-processor-lfr-frequency-provision}"
infra_network="${WAMA_INFRA_NETWORK:-wama-infra}"
runner_url="${FORGEJO_RUNNER_URL:-}"
api_url="${FORGEJO_API_URL:-http://forgejo:3000/api/v1}"
deploy_marker=.wama-forgejo-processor-root
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

initialize_deploy_root() {
  repository="$1"
  deploy_root="$2"
  marker="$deploy_root/$deploy_marker"
  if [ -e "$deploy_root" ] && [ ! -d "$deploy_root" ]; then
    printf '%s\n' "$deploy_root must be a directory" >&2
    exit 1
  fi
  mkdir -p "$deploy_root"
  if [ -e "$marker" ]; then
    if [ -L "$marker" ] || [ ! -f "$marker" ]; then
      printf '%s\n' "$deploy_root has an invalid deployment marker" >&2
      exit 1
    fi
    return
  fi
  if [ -n "$(find "$deploy_root" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
    printf '%s\n' "$deploy_root must be empty before bootstrap creates its marker" >&2
    exit 1
  fi
  printf '%s\n' "Managed by the WAMA Forgejo repository $repository." > "$marker"
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
  scope="$admin_username/$repository"
  repository_url="http://forgejo:3000/$scope.git"
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

reset_runner_state() {
  rm -f \
    "$runner_dir/forgejo-runner.secret" \
    "$runner_dir/forgejo-runner.uuid" \
    "$runner_dir/forgejo-processors.scope" \
    "$runner_dir/forgejo-processors.layout" \
    "$runner_dir"/wama-processor-*.secret \
    "$runner_dir"/wama-processor-*.uuid \
    "$runner_scope_file" \
    "$runner_config_file" \
    "$runner_dir/.runner"
}

register_runner() {
  repository="$1"
  runner_name="$2"
  runner_label="$3"
  runner_secret_file="$runner_dir/$runner_name.secret"
  runner_uuid_file="$runner_dir/$runner_name.uuid"
  scope="$admin_username/$repository"
  if [ ! -s "$runner_secret_file" ]; then
    forgejo_as_git forgejo-cli actions generate-secret > "$runner_secret_file"
  fi
  chown git:git "$runner_secret_file"
  uuid="$(forgejo_as_git forgejo-cli actions register \
    --name "$runner_name" \
    --secret-file "$runner_secret_file" \
    --scope "$scope" \
    --labels "$runner_label" | tr -d '\r\n')"
  if [ -n "$uuid" ]; then
    printf '%s\n' "$uuid" > "$runner_uuid_file"
  fi
  if [ ! -s "$runner_uuid_file" ]; then
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
require_value WAMA_FREQUENCY_SCALE_DEPLOY_ROOT "$frequency_deploy_root"
require_value WAMA_APPARENT_POWER_DEPLOY_ROOT "$apparent_deploy_root"
require_value WAMA_FREQUENCY_IEC104_EXPORT_DEPLOY_ROOT "$frequency_iec104_export_deploy_root"
require_value WAMA_LFR_FREQUENCY_PROVISION_DEPLOY_ROOT "$lfr_frequency_provision_deploy_root"
validate_identifier FORGEJO_BOOTSTRAP_ADMIN_USERNAME "$admin_username"
validate_identifier FORGEJO_FREQUENCY_SCALE_REPOSITORY "$frequency_repository"
validate_identifier FORGEJO_APPARENT_POWER_REPOSITORY "$apparent_repository"
validate_identifier FORGEJO_FREQUENCY_IEC104_EXPORT_REPOSITORY "$frequency_iec104_export_repository"
validate_identifier FORGEJO_LFR_FREQUENCY_PROVISION_REPOSITORY "$lfr_frequency_provision_repository"
validate_deploy_root WAMA_FREQUENCY_SCALE_DEPLOY_ROOT "$frequency_deploy_root"
validate_deploy_root WAMA_APPARENT_POWER_DEPLOY_ROOT "$apparent_deploy_root"
validate_deploy_root WAMA_FREQUENCY_IEC104_EXPORT_DEPLOY_ROOT "$frequency_iec104_export_deploy_root"
validate_deploy_root WAMA_LFR_FREQUENCY_PROVISION_DEPLOY_ROOT "$lfr_frequency_provision_deploy_root"

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
initialize_deploy_root "$frequency_repository" "$frequency_deploy_root"
initialize_deploy_root "$apparent_repository" "$apparent_deploy_root"
initialize_deploy_root "$frequency_iec104_export_repository" "$frequency_iec104_export_deploy_root"
initialize_deploy_root "$lfr_frequency_provision_repository" "$lfr_frequency_provision_deploy_root"
api_auth_header="$(printf '%s:%s' "$admin_username" "$admin_password" | base64 | tr -d '\n')"
ensure_repository "$frequency_repository"
ensure_repository "$apparent_repository"
ensure_repository "$frequency_iec104_export_repository"
ensure_repository "$lfr_frequency_provision_repository"
seed_repository_if_empty "$frequency_repository" "$seed_root/processor-frequency-scale"
seed_repository_if_empty "$apparent_repository" "$seed_root/processor-apparent-power"
seed_repository_if_empty "$frequency_iec104_export_repository" "$seed_root/processor-frequency-iec104-export"
seed_repository_if_empty "$lfr_frequency_provision_repository" "$seed_root/processor-lfr-frequency-provision"
ensure_package_token

scope_manifest="$admin_username/$frequency_repository,$admin_username/$apparent_repository,$admin_username/$frequency_iec104_export_repository,$admin_username/$lfr_frequency_provision_repository"
if [ ! -f "$runner_layout_file" ] || [ "$(cat "$runner_layout_file")" != "$runner_layout" ]; then
  reset_runner_state
fi
if [ -f "$runner_scope_file" ] && [ "$(cat "$runner_scope_file")" != "$scope_manifest" ]; then
  reset_runner_state
fi

frequency_ci_name=wama-processor-frequency-scale-ci
frequency_deploy_name=wama-processor-frequency-scale-deploy
apparent_ci_name=wama-processor-apparent-power-ci
apparent_deploy_name=wama-processor-apparent-power-deploy
frequency_iec104_export_ci_name=wama-processor-frequency-iec104-export-ci
frequency_iec104_export_deploy_name=wama-processor-frequency-iec104-export-deploy
lfr_frequency_provision_ci_name=wama-processor-lfr-frequency-provision-ci
lfr_frequency_provision_deploy_name=wama-processor-lfr-frequency-provision-deploy
register_runner "$frequency_repository" "$frequency_ci_name" "$runner_ci_label"
register_runner "$frequency_repository" "$frequency_deploy_name" "$runner_deploy_label"
register_runner "$apparent_repository" "$apparent_ci_name" "$runner_ci_label"
register_runner "$apparent_repository" "$apparent_deploy_name" "$runner_deploy_label"
register_runner "$frequency_iec104_export_repository" "$frequency_iec104_export_ci_name" "$runner_ci_label"
register_runner "$frequency_iec104_export_repository" "$frequency_iec104_export_deploy_name" "$runner_deploy_label"
register_runner "$lfr_frequency_provision_repository" "$lfr_frequency_provision_ci_name" "$runner_ci_label"
register_runner "$lfr_frequency_provision_repository" "$lfr_frequency_provision_deploy_name" "$runner_deploy_label"

printf '%s\n' "$runner_layout" > "$runner_layout_file"
printf '%s\n' "$scope_manifest" > "$runner_scope_file"
package_token="$(cat "$runner_package_token_file")"
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
    WAMA_INFRA_NETWORK: "$infra_network"
    FORGEJO_PROCESSORS_PACKAGE_USERNAME: "$admin_username"
    FORGEJO_PROCESSORS_PACKAGE_TOKEN: "$package_token"
container:
  docker_host: automount
  valid_volumes:
    - "$frequency_deploy_root"
    - "$apparent_deploy_root"
    - "$frequency_iec104_export_deploy_root"
    - "$lfr_frequency_provision_deploy_root"
  options: "--add-host=host.docker.internal:host-gateway --cpus=2 --memory=2g"
server:
  connections:
    $frequency_ci_name:
      url: $runner_url
      uuid: $(cat "$runner_dir/$frequency_ci_name.uuid")
      token: $(cat "$runner_dir/$frequency_ci_name.secret")
    $frequency_deploy_name:
      url: $runner_url
      uuid: $(cat "$runner_dir/$frequency_deploy_name.uuid")
      token: $(cat "$runner_dir/$frequency_deploy_name.secret")
    $apparent_ci_name:
      url: $runner_url
      uuid: $(cat "$runner_dir/$apparent_ci_name.uuid")
      token: $(cat "$runner_dir/$apparent_ci_name.secret")
    $apparent_deploy_name:
      url: $runner_url
      uuid: $(cat "$runner_dir/$apparent_deploy_name.uuid")
      token: $(cat "$runner_dir/$apparent_deploy_name.secret")
    $frequency_iec104_export_ci_name:
      url: $runner_url
      uuid: $(cat "$runner_dir/$frequency_iec104_export_ci_name.uuid")
      token: $(cat "$runner_dir/$frequency_iec104_export_ci_name.secret")
    $frequency_iec104_export_deploy_name:
      url: $runner_url
      uuid: $(cat "$runner_dir/$frequency_iec104_export_deploy_name.uuid")
      token: $(cat "$runner_dir/$frequency_iec104_export_deploy_name.secret")
    $lfr_frequency_provision_ci_name:
      url: $runner_url
      uuid: $(cat "$runner_dir/$lfr_frequency_provision_ci_name.uuid")
      token: $(cat "$runner_dir/$lfr_frequency_provision_ci_name.secret")
    $lfr_frequency_provision_deploy_name:
      url: $runner_url
      uuid: $(cat "$runner_dir/$lfr_frequency_provision_deploy_name.uuid")
      token: $(cat "$runner_dir/$lfr_frequency_provision_deploy_name.secret")
EOF
