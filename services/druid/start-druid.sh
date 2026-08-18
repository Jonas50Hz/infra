#!/bin/bash

set -euo pipefail

druid_home=/opt/druid
base_config="$druid_home/conf/druid/single-server/nano-quickstart"
runtime_config="$druid_home/var/wama-config"
overlay_config=/opt/wama/config

prepare_config() {
  rm -rf "$runtime_config"
  mkdir -p "$runtime_config" "$druid_home/var/log"
  cp -R "$base_config"/. "$runtime_config"

  cat "$overlay_config/common.runtime.properties" >> "$runtime_config/_common/common.runtime.properties"
  for service in broker coordinator-overlord historical router; do
    cat "$overlay_config/$service.runtime.properties" >> "$runtime_config/$service/runtime.properties"
    cp "$overlay_config/$service.jvm.config" "$runtime_config/$service/jvm.config"
  done
}

child_processes=()

start_process() {
  "$@" &
  child_processes+=("$!")
}

stop_processes() {
  trap - INT TERM
  if ((${#child_processes[@]})); then
    kill -TERM "${child_processes[@]}" 2>/dev/null || true
    wait "${child_processes[@]}" 2>/dev/null || true
  fi
}

shutdown() {
  stop_processes
  exit 0
}

prepare_config
export DRUID_LOG_DIR="$druid_home/var/log"

trap shutdown INT TERM

start_process "$druid_home/bin/run-zk" "$druid_home/conf"
start_process "$druid_home/bin/run-druid" coordinator-overlord "$runtime_config"
start_process "$druid_home/bin/run-druid" historical "$runtime_config"
start_process "$druid_home/bin/run-druid" broker "$runtime_config"
start_process "$druid_home/bin/run-druid" router "$runtime_config"

if wait -n "${child_processes[@]}"; then
  status=0
else
  status=$?
fi
stop_processes
exit "$status"