#!/usr/bin/env bash

set -euo pipefail

if [[ "$#" -gt 2 ]]; then
  printf '%s\n' "usage: measure-memory.sh [duration-seconds] [warmup-seconds]" >&2
  exit 2
fi

duration_seconds="${1:-120}"
warmup_seconds="${2:-60}"
repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
supervisor_id="${DRUID_SUPERVISOR_ID:-live_measurements}"

validate_nonnegative_integer() {
  local value="$1"
  local name="$2"

  if [[ ! "$value" =~ ^[0-9]+$ ]]; then
    printf '%s must be a non-negative integer: %s\n' "$name" "$value" >&2
    exit 2
  fi
}

require_command() {
  local command_name="$1"

  if ! command -v "$command_name" >/dev/null 2>&1; then
    printf 'Required command is unavailable: %s\n' "$command_name" >&2
    exit 2
  fi
}

read_cgroup_memory() {
  docker exec --user root "$container_id" sh -ec '
    if [ -r /sys/fs/cgroup/memory.current ]; then
      cat /sys/fs/cgroup/memory.current
    elif [ -r /sys/fs/cgroup/memory/memory.usage_in_bytes ]; then
      cat /sys/fs/cgroup/memory/memory.usage_in_bytes
    else
      exit 1
    fi
  '
}

print_process_snapshot() {
  local stage="$1"

  docker exec --user root "$container_id" sh -ec '
    for process_directory in /proc/[0-9]*; do
      process_id="${process_directory##*/}"
      process_comm="$(cat "$process_directory/comm" 2>/dev/null || true)"
      command_line="$(tr "\000" " " < "$process_directory/cmdline" 2>/dev/null || true)"
      rss_kib="$(awk "/VmRSS:/ { print \$2; exit }" "$process_directory/status" 2>/dev/null || true)"

      case "$process_comm" in
        java) ;;
        *) continue ;;
      esac

      case "$command_line" in
        *index_kafka*|*KafkaIndexTask*) process_name="local-kafka-task" ;;
        *coordinator-overlord*) process_name="coordinator-overlord" ;;
        *historical*) process_name="historical" ;;
        *broker*) process_name="broker" ;;
        *router*) process_name="router" ;;
        *QuorumPeerMain*|*zookeeper*) process_name="embedded-zookeeper" ;;
        *) process_name="java-other" ;;
      esac

      case "$rss_kib" in
        ""|*[!0-9]*) rss_bytes=0 ;;
        *) rss_bytes=$((rss_kib * 1024)) ;;
      esac
      case "$command_line" in
        *" -cp "*) command_summary="${command_line%% -cp *} <classpath omitted>" ;;
        *) command_summary="$command_line" ;;
      esac
      printf "druid_memory_process stage=%s name=%s pid=%s rss_bytes=%s command=%s\\n" \
        "'"$stage"'" "$process_name" "$process_id" "$rss_bytes" "$command_summary"
    done
  '
}

validate_nonnegative_integer "$duration_seconds" "duration-seconds"
validate_nonnegative_integer "$warmup_seconds" "warmup-seconds"
if ((warmup_seconds > duration_seconds)); then
  printf '%s\n' "warmup-seconds must not exceed duration-seconds" >&2
  exit 2
fi

require_command docker

cd "$repository_root"
cgroup_version="$(docker info --format '{{.CgroupVersion}}' 2>/dev/null || true)"
if [[ "$cgroup_version" != "1" && "$cgroup_version" != "2" ]]; then
  printf '%s\n' "Docker cgroup-memory accounting is required for this measurement." >&2
  exit 1
fi

container_id="$(docker compose ps --quiet druid)"
if [[ -z "$container_id" ]]; then
  printf '%s\n' "The Druid service is not running." >&2
  exit 1
fi

container_state="$(docker inspect --format '{{.State.Running}} {{if .State.Health}}{{.State.Health.Status}}{{end}}' "$container_id")"
if [[ "$container_state" != "true healthy" ]]; then
  printf 'Druid must be healthy before measuring memory: %s\n' "$container_state" >&2
  exit 1
fi

if ! docker exec "$container_id" /busybox/busybox wget -q -O - \
  http://localhost:8888/druid/indexer/v1/supervisor | grep -Fq "\"$supervisor_id\""; then
  printf 'Druid supervisor is not available: %s\n' "$supervisor_id" >&2
  exit 1
fi

printf 'druid_memory_measurement container_id=%s cgroup_version=%s duration_seconds=%s warmup_seconds=%s supervisor_id=%s\n' \
  "$container_id" "$cgroup_version" "$duration_seconds" "$warmup_seconds" "$supervisor_id"
print_process_snapshot "start"

peak_cgroup_bytes=0
warmup_cgroup_bytes=0
final_cgroup_bytes=0
sample_count=0

for ((elapsed_seconds = 0; elapsed_seconds <= duration_seconds; elapsed_seconds++)); do
  cgroup_bytes="$(read_cgroup_memory 2>/dev/null || true)"
  if [[ ! "$cgroup_bytes" =~ ^[0-9]+$ ]]; then
    printf '%s\n' "Cannot read Druid container cgroup memory." >&2
    exit 1
  fi

  if ((cgroup_bytes > peak_cgroup_bytes)); then
    peak_cgroup_bytes="$cgroup_bytes"
  fi
  if ((elapsed_seconds == warmup_seconds)); then
    warmup_cgroup_bytes="$cgroup_bytes"
  fi
  final_cgroup_bytes="$cgroup_bytes"
  ((sample_count += 1))

  if ((elapsed_seconds < duration_seconds)); then
    sleep 1
  fi
done

print_process_snapshot "end"
printf 'druid_memory_result samples=%s warmup_cgroup_bytes=%s peak_cgroup_bytes=%s final_cgroup_bytes=%s\n' \
  "$sample_count" "$warmup_cgroup_bytes" "$peak_cgroup_bytes" "$final_cgroup_bytes"