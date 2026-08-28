#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
infra_network="${WAMA_INFRA_NETWORK:-wama-infra}"

readonly -a deployed_project_names=(
  "wama-processor-frequency-scale"
  "wama-processor-apparent-power"
  "wama-processor-frequency-iec104-export"
  "wama-processor-frequency-measurement-session"
  "wama-processor-alarm-threshold"
  "wama-gateway-c37-118"
  "c37-118-simulator"
)

usage() {
  cat <<'EOF'
Usage: scripts/reset-wama.sh --yes

Stops and removes all containers attached to the WAMA infrastructure network,
known Forgejo-deployed WAMA projects, and their volumes. It then removes the
root Compose stack's declared volumes and network.

The command does not remove images or Docker resources outside this WAMA setup.
Set WAMA_INFRA_NETWORK when the stack uses a nondefault network name.
EOF
}

confirm_reset() {
  local answer

  if [[ "${1:-}" == "--yes" ]]; then
    return 0
  fi

  printf 'This permanently removes local WAMA containers and data. Type RESET to continue: '
  read -r answer
  [[ "$answer" == "RESET" ]]
}

collect_container() {
  local container_id="$1"

  [[ -n "$container_id" ]] || return 0
  target_containers["$container_id"]=1
}

collect_project_containers() {
  local project_name="$1"
  local container_id

  while IFS= read -r container_id; do
    collect_container "$container_id"
  done < <(docker ps --all --quiet --filter "label=com.docker.compose.project=$project_name")
}

collect_project_volumes() {
  local project_name="$1"
  local volume_name

  while IFS= read -r volume_name; do
    [[ -n "$volume_name" ]] || continue
    target_volumes["$volume_name"]=1
  done < <(docker volume ls --quiet --filter "label=com.docker.compose.project=$project_name")
}

collect_network_containers() {
  local container_id

  docker network inspect "$infra_network" >/dev/null 2>&1 || return 0
  while IFS= read -r container_id; do
    collect_container "$container_id"
  done < <(
    docker network inspect --format '{{range $id, $_ := .Containers}}{{$id}}{{"\n"}}{{end}}' \
      "$infra_network"
  )
}

collect_container_volumes() {
  local container_id="$1"
  local volume_name

  while IFS= read -r volume_name; do
    [[ -n "$volume_name" ]] || continue
    target_volumes["$volume_name"]=1
  done < <(
    docker inspect --format '{{range .Mounts}}{{if eq .Type "volume"}}{{.Name}}{{"\n"}}{{end}}{{end}}' \
      "$container_id"
  )
}

remove_collected_containers() {
  local container_id

  for container_id in "${!target_containers[@]}"; do
    collect_container_volumes "$container_id"
  done

  if ((${#target_containers[@]})); then
    docker rm --force --volumes "${!target_containers[@]}"
  fi
}

remove_collected_volumes() {
  local volume_name

  for volume_name in "${!target_volumes[@]}"; do
    if docker volume inspect "$volume_name" >/dev/null 2>&1; then
      docker volume rm "$volume_name"
    fi
  done
}

if (($# > 1)) || { (($# == 1)) && [[ "$1" != "--yes" && "$1" != "--help" && "$1" != "-h" ]]; }; then
  usage >&2
  exit 2
fi

if (($# == 1)) && [[ "$1" == "--help" || "$1" == "-h" ]]; then
  usage
  exit 0
fi

cd "$repository_root"
docker compose config --quiet

if ! confirm_reset "${1:-}"; then
  printf '%s\n' "Reset cancelled."
  exit 1
fi

declare -A target_containers=()
declare -A target_volumes=()

for project_name in "${deployed_project_names[@]}"; do
  collect_project_containers "$project_name"
  collect_project_volumes "$project_name"
done
collect_network_containers

remove_collected_containers
docker compose down --volumes --remove-orphans
remove_collected_volumes

if docker network inspect "$infra_network" >/dev/null 2>&1; then
  docker network rm "$infra_network"
fi

printf '%s\n' "WAMA local state has been removed."