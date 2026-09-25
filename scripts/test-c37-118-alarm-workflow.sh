#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

usage() {
  printf '%s\n' \
    'Usage:' \
    '  WAMA_RUN_C37_118_ALARM_WORKFLOW_TEST=run-c37-118-alarm-workflow-test \' \
    '  scripts/test-c37-118-alarm-workflow.sh'
}

if [[ "${WAMA_RUN_C37_118_ALARM_WORKFLOW_TEST:-}" != "run-c37-118-alarm-workflow-test" ]]; then
  printf '%s\n' \
    'This workflow activates a real transient simulator excursion and requires explicit arming.' >&2
  usage >&2
  exit 2
fi

timeout_seconds="${WAMA_C37_118_ALARM_WORKFLOW_TIMEOUT_SECONDS:-90}"
if [[ ! "$timeout_seconds" =~ ^[1-9][0-9]*$ ]]; then
  printf 'WAMA_C37_118_ALARM_WORKFLOW_TIMEOUT_SECONDS must be a positive integer.\n' >&2
  exit 2
fi

simulator_console_url="${C37_118_SIMULATOR_CONSOLE_URL:-http://127.0.0.1:8081}"
alerta_api_url="${ALERTA_API_URL:-http://127.0.0.1:18081/api}"
stream_id="${C37_118_ALARM_WORKFLOW_STREAM_ID:-1001}"
rule_id="${C37_118_ALARM_WORKFLOW_RULE_ID:-frequency-over-50-1-hz}"
mrid="${C37_118_ALARM_WORKFLOW_MRID:-urn:wama:poc:pmu:bay-01:frequency}"
operator_label="${C37_118_ALARM_WORKFLOW_OPERATOR_LABEL:-alarm-workflow-$(python3 -c 'import uuid; print(uuid.uuid4())')}"

if [[ ! "$stream_id" =~ ^[1-9][0-9]*$ || "$stream_id" -gt 65535 ]]; then
  printf 'C37_118_ALARM_WORKFLOW_STREAM_ID must be an integer from 1 through 65535.\n' >&2
  exit 2
fi

if [[ -z "$rule_id" || -z "$mrid" ]]; then
  printf 'C37_118_ALARM_WORKFLOW_RULE_ID and C37_118_ALARM_WORKFLOW_MRID must be nonempty.\n' >&2
  exit 2
fi

if (( $(printf '%s' "$operator_label" | wc -c | tr -d ' ') > 64 )); then
  printf 'C37_118_ALARM_WORKFLOW_OPERATOR_LABEL must be at most 64 UTF-8 bytes.\n' >&2
  exit 2
fi

wait_for_http() {
  local url="$1"
  local description="$2"
  local deadline=$((SECONDS + timeout_seconds))

  while (( SECONDS < deadline )); do
    if curl --fail --silent --show-error "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  printf 'Timed out waiting for %s at %s.\n' "$description" "$url" >&2
  return 1
}

require_running_service() {
  local service="$1"
  local identifiers
  local identifier

  identifiers="$(docker ps --quiet --filter "label=com.docker.compose.service=${service}")"
  if [[ -z "$identifiers" ]]; then
    printf 'Required Compose service %s is not running.\n' "$service" >&2
    return 1
  fi
  while IFS= read -r identifier; do
    if [[ -n "$identifier" ]] && [[ "$(docker inspect --format '{{.State.Running}}' "$identifier")" == "true" ]]; then
      return 0
    fi
  done <<< "$identifiers"
  printf 'Required Compose service %s has no running container.\n' "$service" >&2
  return 1
}

require_root_readiness() {
  local readiness_id
  local readiness_ids
  local readiness_state
  local readiness_exit_code

  readiness_ids="$(docker ps --all --quiet --filter 'label=com.docker.compose.service=infra-readiness')"
  if [[ -z "$readiness_ids" ]]; then
    printf 'Root infra-readiness service is absent.\n' >&2
    return 1
  fi
  if [[ "$(printf '%s\n' "$readiness_ids" | wc -l | tr -d ' ')" != "1" ]]; then
    printf 'Expected exactly one root infra-readiness container.\n' >&2
    return 1
  fi
  readiness_id="$readiness_ids"
  readiness_state="$(docker inspect --format '{{.State.Status}}' "$readiness_id")"
  readiness_exit_code="$(docker inspect --format '{{.State.ExitCode}}' "$readiness_id")"
  if [[ "$readiness_state" != "exited" || "$readiness_exit_code" != "0" ]]; then
    printf 'Root infra-readiness must have completed successfully before this workflow test.\n' >&2
    return 1
  fi
}

canonical_alarm_key() {
  python3 - "$rule_id" "$mrid" <<'PY'
import base64
import sys

rule_id, mrid = sys.argv[1:]
encode = lambda value: base64.urlsafe_b64encode(value.encode("utf-8")).rstrip(b"=").decode("ascii")
print(f"alarm/v1/{encode(rule_id)}/{encode(mrid)}")
PY
}

scenario_body() {
  python3 - "$stream_id" "$operator_label" <<'PY'
import json
import sys

stream_id, operator_label = sys.argv[1:]
print(json.dumps(
    {
        "target": {"stream_id": int(stream_id)},
        "scenario_name": "signal-excursion",
        "actor_label": operator_label,
    },
    ensure_ascii=True,
    separators=(",", ":"),
))
PY
}

confirmation_body() {
  local token="$1"
  python3 - "$token" "$operator_label" <<'PY'
import json
import sys

token, operator_label = sys.argv[1:]
if not token.isascii() or not token.isdigit() or int(token) <= 0:
    raise SystemExit("Simulator returned an invalid scenario confirmation token")
print(json.dumps(
    {"token": token, "actor_label": operator_label},
    ensure_ascii=True,
    separators=(",", ":"),
))
PY
}

confirmation_token() {
  local payload="$1"
  python3 - "$payload" <<'PY'
import json
import sys

payload = json.loads(sys.argv[1])
token = payload.get("token")
expires = payload.get("confirm_expires_in_ms")
if not isinstance(token, str) or not token.isascii() or not token.isdigit() or int(token) <= 0:
    raise SystemExit("Simulator prepare response has no canonical positive token")
if isinstance(expires, bool) or not isinstance(expires, int) or expires <= 0:
    raise SystemExit("Simulator prepare response has no positive confirmation expiry")
print(token)
PY
}

find_open_alert() {
  local payload

  payload="$(
    curl --fail --silent --show-error \
      "${alerta_api_url%/}/alerts?environment=WAMA&status=open&status=ack&page-size=1000"
  )" || return $?
  python3 - "$alarm_key" "$payload" <<'PY'
import json
import sys

alarm_key = sys.argv[1]
payload = json.loads(sys.argv[2])
alerts = payload.get("alerts")
if not isinstance(alerts, list):
    raise SystemExit("Alerta response has no alerts list")
for alert in alerts:
    if not isinstance(alert, dict):
        continue
    attributes = alert.get("attributes")
    if not isinstance(attributes, dict) or attributes.get("wama_alarm_key") != alarm_key:
        continue
    if alert.get("status") in {"open", "ack"}:
        identifier = alert.get("id")
        event = alert.get("event")
        if not isinstance(identifier, str) or not identifier:
            raise SystemExit("Matching active Alerta incident has no ID")
        if not isinstance(event, str) or not event:
            raise SystemExit("Matching active Alerta incident has no event")
        print(f"{identifier} {alert['status']} {event}")
        raise SystemExit(0)
raise SystemExit(3)
PY
}

alert_status() {
  local alert_id="$1"
  local payload

  payload="$(
    curl --fail --silent --show-error \
      "${alerta_api_url%/}/alerts?environment=WAMA&status=closed&page-size=1000"
  )" || return $?
  python3 - "$alert_id" "$payload" <<'PY'
import json
import sys

alert_id = sys.argv[1]
payload = json.loads(sys.argv[2])
alerts = payload.get("alerts")
if not isinstance(alerts, list):
    raise SystemExit("Alerta response has no alerts list")
for alert in alerts:
    if isinstance(alert, dict) and alert.get("id") == alert_id:
        status = alert.get("status")
        if not isinstance(status, str) or not status:
            raise SystemExit("Matching Alerta incident has no status")
        print(status)
        raise SystemExit(0)
raise SystemExit(3)
PY
}

wait_for_open_alert() {
  local deadline=$((SECONDS + timeout_seconds))
  local result
  local status

  while (( SECONDS < deadline )); do
    if result="$(find_open_alert)"; then
      printf '%s\n' "$result"
      return 0
    else
      status=$?
    fi
    if (( status != 3 )); then
      return "$status"
    fi
    sleep 1
  done
  printf 'Timed out waiting for the high-frequency WAMA incident to open.\n' >&2
  return 1
}

wait_for_closed_alert() {
  local alert_id="$1"
  local deadline=$((SECONDS + timeout_seconds))
  local status
  local status_code

  while (( SECONDS < deadline )); do
    if status="$(alert_status "$alert_id")"; then
      if [[ "$status" == "closed" ]]; then
        return 0
      fi
    else
      status_code=$?
      if (( status_code != 3 )); then
        return "$status_code"
      fi
    fi
    sleep 1
  done
  printf 'Timed out waiting for Alerta incident %s to close after the excursion.\n' "$alert_id" >&2
  return 1
}

require_root_readiness
require_running_service "processor-alarm-threshold"
require_running_service "c37-118-gateway-pmu-bay-01"
wait_for_http "${simulator_console_url%/}/api/readyz" "simulator management readiness"
wait_for_http "${alerta_api_url%/}/management/gtg" "Alerta readiness"

alarm_key="$(canonical_alarm_key)"
if existing="$(find_open_alert)"; then
  printf 'Refusing to overlap an already active Alarm identity: %s\n' "$existing" >&2
  exit 1
else
  find_status=$?
  if (( find_status != 3 )); then
    exit "$find_status"
  fi
fi

prepare_response="$(
  curl --fail --silent --show-error \
    --request POST \
    --header 'Content-Type: application/json' \
    --data "$(scenario_body)" \
    "${simulator_console_url%/}/api/v1/scenarios/prepare"
)"
token="$(confirmation_token "$prepare_response")"

curl --fail --silent --show-error \
  --request POST \
  --header 'Content-Type: application/json' \
  --data "$(confirmation_body "$token")" \
  "${simulator_console_url%/}/api/v1/scenarios/confirm" >/dev/null

opened="$(wait_for_open_alert)"
alert_id="${opened%% *}"
wait_for_closed_alert "$alert_id"

printf 'Verified C37.118 high-frequency path: stream %s -> %s -> Alerta incident %s (closed after excursion).\n' \
  "$stream_id" "$mrid" "$alert_id"
