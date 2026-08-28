#!/usr/bin/env bash

set -euo pipefail

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repository_root"

cleanup_only_usage() {
  printf '%s\n' \
    'Cleanup-only usage:' \
    '  WAMA_RUN_ROOT_ALARM_FLOW_TEST=run-root-alarm-flow-test \' \
    '  ALERTA_ALARM_FLOW_CLEANUP_ONLY=cleanup-only \' \
    '  ALERTA_ALARM_FLOW_CLEANUP_CONFIRM=delete-confirmed-mailpit-test-artifacts \' \
    '  ALERTA_ALARM_FLOW_RUN_ID=<run-id> \' \
    '  ALERTA_ALARM_FLOW_EPISODE_ONE=<episode-one> \' \
    '  ALERTA_ALARM_FLOW_EPISODE_TWO=<episode-two> \' \
    '  scripts/test-alerta-alarm-flow.sh'
}

validate_cleanup_only_invocation() {
  if [[ "${WAMA_RUN_ROOT_ALARM_FLOW_TEST:-}" != "run-root-alarm-flow-test" ]]; then
    printf '%s\n' "Cleanup-only mode requires WAMA_RUN_ROOT_ALARM_FLOW_TEST=run-root-alarm-flow-test." >&2
    cleanup_only_usage >&2
    return 1
  fi
  if [[ "${ALERTA_ALARM_FLOW_CLEANUP_CONFIRM:-}" != "delete-confirmed-mailpit-test-artifacts" ]]; then
    printf '%s\n' \
      "Cleanup-only mode requires ALERTA_ALARM_FLOW_CLEANUP_CONFIRM=delete-confirmed-mailpit-test-artifacts." >&2
    cleanup_only_usage >&2
    return 1
  fi
  if [[ -z "${ALERTA_ALARM_FLOW_RUN_ID:-}" || -z "${ALERTA_ALARM_FLOW_EPISODE_ONE:-}" || -z "${ALERTA_ALARM_FLOW_EPISODE_TWO:-}" ]]; then
    printf '%s\n' \
      "Cleanup-only mode requires nonempty ALERTA_ALARM_FLOW_RUN_ID, ALERTA_ALARM_FLOW_EPISODE_ONE, and ALERTA_ALARM_FLOW_EPISODE_TWO." >&2
    cleanup_only_usage >&2
    return 1
  fi
  if [[ "$ALERTA_ALARM_FLOW_EPISODE_ONE" == "$ALERTA_ALARM_FLOW_EPISODE_TWO" ]]; then
    printf '%s\n' "ALERTA_ALARM_FLOW_EPISODE_ONE and ALERTA_ALARM_FLOW_EPISODE_TWO must differ." >&2
    cleanup_only_usage >&2
    return 1
  fi
}

cleanup_only=false
cleanup_only_mode="${ALERTA_ALARM_FLOW_CLEANUP_ONLY:-}"
if [[ "$cleanup_only_mode" == "cleanup-only" ]]; then
  cleanup_only=true
elif [[ -n "$cleanup_only_mode" ]]; then
  printf '%s\n' "Unsupported ALERTA_ALARM_FLOW_CLEANUP_ONLY value: $cleanup_only_mode" >&2
  cleanup_only_usage >&2
  exit 2
fi

if [[ "$cleanup_only" == true ]]; then
  if ! validate_cleanup_only_invocation; then
    exit 2
  fi
  run_id="$ALERTA_ALARM_FLOW_RUN_ID"
  episode_one="$ALERTA_ALARM_FLOW_EPISODE_ONE"
  episode_two="$ALERTA_ALARM_FLOW_EPISODE_TWO"
else
  run_id="${ALERTA_ALARM_FLOW_RUN_ID:-$(cat /proc/sys/kernel/random/uuid)}"
  episode_one="${ALERTA_ALARM_FLOW_EPISODE_ONE:-$(cat /proc/sys/kernel/random/uuid)}"
  episode_two="${ALERTA_ALARM_FLOW_EPISODE_TWO:-$(cat /proc/sys/kernel/random/uuid)}"
fi
isolated=true
mailpit_helper_self_test=false
cleanup_only_self_test=false
if [[ "${WAMA_RUN_ROOT_ALARM_FLOW_TEST:-}" == "run-root-alarm-flow-test" ]]; then
  isolated=false
fi
if [[ "${ALERTA_ALARM_FLOW_MAILPIT_HELPER_TEST:-}" == "1" ]]; then
  mailpit_helper_self_test=true
fi
if [[ "${ALERTA_ALARM_FLOW_CLEANUP_ONLY_SELF_TEST:-}" == "1" ]]; then
  cleanup_only_self_test=true
fi

if [[ "$isolated" == true ]]; then
  compose_command=(
    docker compose
    --project-name "wama-alerta-flow-${run_id//-/}"
    --file services/alarm-alerta-ingress/test-compose.yaml
  )
else
  compose_command=(docker compose)
fi

compose() {
  "${compose_command[@]}" "$@"
}

verify_root_stack_ready() {
  local readiness_id
  local readiness_state
  local readiness_exit_code

  readiness_id="$(compose ps -aq infra-readiness)"
  if [[ -z "$readiness_id" ]]; then
    printf '%s\n' \
      "Root mode requires a successful infra-readiness service before it can mutate root services." >&2
    return 1
  fi
  readiness_state="$(docker inspect --format '{{.State.Status}}' "$readiness_id")"
  readiness_exit_code="$(docker inspect --format '{{.State.ExitCode}}' "$readiness_id")"
  if [[ "$readiness_state" != "exited" || "$readiness_exit_code" != "0" ]]; then
    printf '%s\n' \
      "Root mode requires infra-readiness to have exited successfully before it can mutate root services." >&2
    return 1
  fi
}

published_host_port() {
  local service="$1"
  local container_port="$2"
  local published_address
  local host_port

  published_address="$(compose port "$service" "$container_port")"
  host_port="${published_address##*:}"
  if [[ ! "$host_port" =~ ^[0-9]+$ ]]; then
    printf 'Cannot determine the published host port for %s:%s.\n' \
      "$service" "$container_port" >&2
    return 1
  fi
  printf '%s\n' "$host_port"
}

first_available_docker_port() {
  local candidate

  for candidate in "$@"; do
    if ! docker ps --format '{{.Ports}}' | grep --quiet --fixed-strings ":${candidate}->"; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done

  return 1
}

if [[ "$isolated" == true && "$mailpit_helper_self_test" == false && "$cleanup_only_self_test" == false ]]; then
  if [[ -z "${ALERTA_HOST_PORT:-}" ]]; then
    export ALERTA_HOST_PORT="$(first_available_docker_port 8081 18081 18082 18083)"
  fi
  if [[ -z "${MAILPIT_UI_PORT:-}" ]]; then
    export MAILPIT_UI_PORT="$(first_available_docker_port 8025 18025 18026 18027)"
  fi
fi
rule_id="alerta-flow-${run_id}"
mrid="urn:wama:poc:test:alerta:${run_id}"
foreign_event="foreign-alerta-flow/${run_id}"
foreign_resource="urn:wama:poc:test:foreign:${run_id}"
alerta_api=""
alerta_api_key="${ALERTA_API_KEY:-wama-alerta-ingress-local-api-key-0001}"
mailpit_api=""
foreign_alert_id=""
ingress_ready=false

if [[ "$cleanup_only" == false && "$mailpit_helper_self_test" == false && "$episode_one" == "$episode_two" ]]; then
  printf '%s\n' "ALERTA_ALARM_FLOW_EPISODE_ONE and ALERTA_ALARM_FLOW_EPISODE_TWO must differ" >&2
  exit 2
fi

show_failure_diagnostics() {
  if [[ "$isolated" == true ]]; then
    printf '%s\n' "Isolated Alerta Alarm flow validation failed; temporary services will be removed."
  else
    printf '%s\n' "Alerta Alarm flow validation failed; leaving root services running for inspection."
  fi
  compose ps --all || true
  compose logs --tail 200 \
    alarm-alerta-ingress \
    alerta \
    alerta-postgres \
    mailpit \
    kafka || true
}

close_foreign_alert() {
  if [[ -z "$foreign_alert_id" ]]; then
    return 0
  fi
  curl --fail --silent --show-error \
    --request PUT \
    --header "Authorization: Key ${alerta_api_key}" \
    --header 'Content-Type: application/json' \
    --data '{"status":"closed","text":"Focused WAMA alarm-flow test cleanup"}' \
    "${alerta_api}/alert/${foreign_alert_id}/status" >/dev/null || true
  foreign_alert_id=""
}

cleanup_fixtures() {
  local verify_mailpit_cleanup="${1:-false}"

  if [[ "$ingress_ready" == true ]]; then
    publish_tombstone "$episode_two" || true
  fi
  close_foreign_alert
  if [[ "$verify_mailpit_cleanup" == true ]]; then
    cleanup_mailpit_messages || return 1
  else
    cleanup_mailpit_messages || true
  fi
  if [[ "$isolated" == true ]]; then
    compose down --volumes --remove-orphans || true
  fi
}

wait_for_http() {
  local url="$1"
  local description="$2"
  local deadline=$((SECONDS + ${ALERTA_ALARM_FLOW_TIMEOUT_SECONDS:-90}))

  while ((SECONDS < deadline)); do
    if curl --fail --silent --show-error "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  printf 'Timed out waiting for %s at %s\n' "$description" "$url" >&2
  return 1
}

wait_for_service_health() {
  local service="$1"
  local deadline=$((SECONDS + ${ALERTA_ALARM_FLOW_TIMEOUT_SECONDS:-90}))
  local container_id
  local health

  while ((SECONDS < deadline)); do
    container_id="$(compose ps -q "$service")"
    if [[ -n "$container_id" ]]; then
      health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$container_id")"
      if [[ "$health" == "healthy" ]]; then
        return 0
      fi
    fi
    sleep 1
  done

  printf 'Timed out waiting for %s to become healthy.\n' "$service" >&2
  return 1
}

wait_for_service_success() {
  local service="$1"
  local deadline=$((SECONDS + ${ALERTA_ALARM_FLOW_TIMEOUT_SECONDS:-90}))
  local container_id
  local state
  local exit_code

  while ((SECONDS < deadline)); do
    container_id="$(compose ps -q "$service")"
    if [[ -n "$container_id" ]]; then
      state="$(docker inspect --format '{{.State.Status}}' "$container_id")"
      exit_code="$(docker inspect --format '{{.State.ExitCode}}' "$container_id")"
      if [[ "$state" == "exited" && "$exit_code" == "0" ]]; then
        return 0
      fi
      if [[ "$state" == "exited" ]]; then
        printf '%s exited with status %s.\n' "$service" "$exit_code" >&2
        return 1
      fi
    fi
    sleep 1
  done

  printf 'Timed out waiting for %s to complete successfully.\n' "$service" >&2
  return 1
}

wait_for_ingress() {
  wait_for_service_health alarm-alerta-ingress
}

publish_active() {
  local episode_id="$1"
  local severity="$2"
  local revision="$3"
  local summary="$4"

  compose run --rm --no-deps -T \
    --entrypoint python \
    -e ALARM_TEST_ACTION=active \
    -e ALARM_TEST_EPISODE_ID="$episode_id" \
    -e ALARM_TEST_MRID="$mrid" \
    -e ALARM_TEST_RULE_ID="$rule_id" \
    -e ALARM_TEST_RULE_REVISION="$revision" \
    -e ALARM_TEST_SEVERITY="$severity" \
    -e ALARM_TEST_SUMMARY="$summary" \
    alarm-alerta-ingress - <<'PY'
from datetime import datetime, timezone
import os

from kafka import KafkaProducer

from alarm_alerta_ingress.codec import canonical_alarm_key
from alarm_alerta_ingress.generated import alarm_pb2

rule_id = os.environ["ALARM_TEST_RULE_ID"]
mrid = os.environ["ALARM_TEST_MRID"]
alarm_key = canonical_alarm_key(rule_id, mrid)
message = alarm_pb2.AlarmDesiredState(
    alarm_key=alarm_key,
    episode_id=os.environ["ALARM_TEST_EPISODE_ID"],
    rule_id=rule_id,
    mrid=mrid,
    severity={
        "WARNING": alarm_pb2.ALARM_SEVERITY_WARNING,
        "CRITICAL": alarm_pb2.ALARM_SEVERITY_CRITICAL,
    }[os.environ["ALARM_TEST_SEVERITY"]],
    rule_revision=os.environ["ALARM_TEST_RULE_REVISION"],
)
observed_at = datetime.now(timezone.utc)
message.activated_at.FromDatetime(observed_at)
message.current_evidence.observed_at.FromDatetime(observed_at)
message.current_evidence.summary = os.environ["ALARM_TEST_SUMMARY"]
attribute = message.current_evidence.attributes.add()
attribute.name = "test_run"
attribute.value = os.environ["ALARM_TEST_EPISODE_ID"]
producer = KafkaProducer(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"])
try:
    producer.send("Alarm", key=alarm_key.encode("utf-8"), value=message.SerializeToString()).get(timeout=30)
finally:
    producer.close()
PY
}

publish_tombstone() {
  local ignored_episode_id="$1"

  compose run --rm --no-deps -T \
    --entrypoint python \
    -e ALARM_TEST_MRID="$mrid" \
    -e ALARM_TEST_RULE_ID="$rule_id" \
    alarm-alerta-ingress - <<'PY'
import os

from kafka import KafkaProducer

from alarm_alerta_ingress.codec import canonical_alarm_key

alarm_key = canonical_alarm_key(
    os.environ["ALARM_TEST_RULE_ID"],
    os.environ["ALARM_TEST_MRID"],
)
producer = KafkaProducer(bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"])
try:
    producer.send("Alarm", key=alarm_key.encode("utf-8"), value=None).get(timeout=30)
finally:
    producer.close()
PY
}

alert_state() {
  local episode_id="$1"

  curl --fail --silent --show-error "${alerta_api}/alerts?environment=WAMA" \
    | python3 -c '
import json
import sys

episode_id = sys.argv[1]
for alert in json.load(sys.stdin).get("alerts", []):
    if alert.get("event") == f"wama-alarm/{episode_id}":
        print(f"{alert.get('"'"'id'"'"', '')} {alert.get('"'"'status'"'"', '')}")
        break
' "$episode_id"
}

wait_for_alert_state() {
  local episode_id="$1"
  local expected_status="$2"
  local deadline=$((SECONDS + ${ALERTA_ALARM_FLOW_TIMEOUT_SECONDS:-90}))
  local state

  while ((SECONDS < deadline)); do
    state="$(alert_state "$episode_id" || true)"
    if [[ "$state" == *" ${expected_status}" ]]; then
      printf '%s\n' "$state"
      return 0
    fi
    sleep 1
  done

  printf 'Timed out waiting for episode %s to become %s\n' "$episode_id" "$expected_status" >&2
  return 1
}

mailpit_episode_count() {
  local episode_id="$1"

  mailpit_messages_for_current_run count-episode "$episode_id"
}

mailpit_expected_subject() {
  local episode_id="$1"

  printf '[WAMA] %s wama-alarm/%s\n' "$mrid" "$episode_id"
}

mailpit_messages_for_current_run() {
  local action="$1"
  shift
  local episode_one_subject
  local episode_two_subject

  if [[ -z "$mailpit_api" ]]; then
  return 0
  fi
  episode_one_subject="$(mailpit_expected_subject "$episode_one")"
  episode_two_subject="$(mailpit_expected_subject "$episode_two")"
  python3 - "$action" "$mailpit_api" "$episode_one_subject" "$episode_two_subject" "$@" <<'PY'
import json
import sys
import urllib.error
import urllib.parse
import urllib.request


PAGE_LIMIT = 50
MAX_PAGE_REQUESTS = 10_000


def fail(message: str) -> None:
  raise RuntimeError(message)


def request(url: str, method: str = "GET", body: bytes | None = None) -> bytes:
  headers = {"Content-Type": "application/json"} if body is not None else {}
  http_request = urllib.request.Request(url, data=body, headers=headers, method=method)
  try:
    with urllib.request.urlopen(http_request, timeout=30) as response:
      if not 200 <= response.status < 300:
        fail(f"Mailpit {method} {url} returned HTTP {response.status}")
      return response.read()
  except urllib.error.HTTPError as error:
    detail = error.read().decode("utf-8", errors="replace")
    fail(f"Mailpit {method} {url} returned HTTP {error.code}: {detail}")
  except (urllib.error.URLError, OSError) as error:
    fail(f"Mailpit {method} {url} failed: {error}")


def nonnegative_integer(payload: dict[str, object], field: str) -> int:
  value = payload.get(field)
  if isinstance(value, bool) or not isinstance(value, int) or value < 0:
    fail(f"Mailpit response has invalid {field!r} pagination value")
  return value


def list_messages(api_url: str, visit_message: object) -> None:
  messages_url = f"{api_url.rstrip('/')}/messages"
  offset = 0
  total: int | None = None
  visited_offsets: set[int] = set()

  for _ in range(MAX_PAGE_REQUESTS):
    if offset in visited_offsets:
      fail(f"Mailpit pagination repeated offset {offset}")
    visited_offsets.add(offset)
    query = urllib.parse.urlencode({"start": offset, "limit": PAGE_LIMIT})
    response_bytes = request(f"{messages_url}?{query}")
    try:
      response = json.loads(response_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
      fail(f"Mailpit returned malformed JSON: {error}")
    if not isinstance(response, dict):
      fail("Mailpit response must be a JSON object")

    page_total = nonnegative_integer(response, "total")
    page_count = nonnegative_integer(response, "count")
    page_start = nonnegative_integer(response, "start")
    messages = response.get("messages")
    if not isinstance(messages, list):
      fail("Mailpit response has no messages list")
    if page_start != offset:
      fail(f"Mailpit response start {page_start} did not match requested offset {offset}")
    if page_count != len(messages) or page_count > PAGE_LIMIT:
      fail("Mailpit response count does not match its messages page")
    if total is None:
      total = page_total
    elif page_total != total:
      fail("Mailpit total changed while paginating")
    if offset > page_total or offset + page_count > page_total:
      fail("Mailpit response pagination bounds are invalid")

    for message in messages:
      if not isinstance(message, dict):
        fail("Mailpit response contains a non-object message")
      visit_message(message)

    next_offset = offset + page_count
    if next_offset == page_total:
      return
    if page_count == 0:
      fail("Mailpit pagination cannot advance before reaching total")
    offset = next_offset

  fail(f"Mailpit pagination exceeded {MAX_PAGE_REQUESTS} requests")


def matching_message_ids(api_url: str, subjects: set[str]) -> list[str]:
  message_ids: list[str] = []
  seen_ids: set[str] = set()

  def collect(message: dict[str, object]) -> None:
    if message.get("Subject") not in subjects:
      return
    message_id = message.get("ID")
    if not isinstance(message_id, str) or not message_id:
      fail("Matching Mailpit message has no usable string ID")
    if message_id in seen_ids:
      fail("Mailpit returned the same matching message ID more than once")
    seen_ids.add(message_id)
    message_ids.append(message_id)

  list_messages(api_url, collect)
  return message_ids


def delete_message(api_url: str, message_id: str) -> None:
  if not message_id:
    fail("Refusing to delete a Mailpit message with an empty ID")
  body = json.dumps({"IDs": [message_id]}, ensure_ascii=True, separators=(",", ":")).encode("utf-8")
  request(f"{api_url.rstrip('/')}/messages", method="DELETE", body=body)


def count_episode_messages(api_url: str, episode_id: str) -> int:
  count = 0

  def count_message(message: dict[str, object]) -> None:
    nonlocal count
    if episode_id in str(message.get("Subject", "")):
      count += 1

  list_messages(api_url, count_message)
  return count


def main() -> None:
  action, api_url, episode_one_subject, episode_two_subject, *arguments = sys.argv[1:]
  subjects = {episode_one_subject, episode_two_subject}
  if len(subjects) != 2:
    fail("Mailpit cleanup subjects must be distinct")

  if action == "count-episode":
    if len(arguments) != 1:
      fail("Mailpit episode count requires exactly one episode ID")
    print(count_episode_messages(api_url, arguments[0]))
    return
  if action != "cleanup-current-run" or arguments:
    fail(f"Unsupported Mailpit helper action {action!r}")

  for message_id in matching_message_ids(api_url, subjects):
    delete_message(api_url, message_id)
  remaining_ids = matching_message_ids(api_url, subjects)
  if remaining_ids:
    fail("Mailpit retained messages for the focused alarm-flow run")


try:
  main()
except RuntimeError as error:
  print(f"Mailpit helper failed: {error}", file=sys.stderr)
  raise SystemExit(1)
PY
}

cleanup_mailpit_messages() {
  mailpit_messages_for_current_run cleanup-current-run
}

run_mailpit_cleanup_helper_self_test() (
  set -euo pipefail

  local test_directory
  local server_pid=""
  local port

  test_directory="$(mktemp -d)"
  cleanup_self_test() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" >/dev/null 2>&1 || true
    wait "$server_pid" >/dev/null 2>&1 || true
  fi
  rm -rf "$test_directory"
  }
  trap cleanup_self_test EXIT

  mrid="urn:wama:poc:test:alerta:mailpit-helper"
  episode_one="mailpit-helper-current"
  episode_two="mailpit-helper-older"
  python3 - "$test_directory" \
  "$(mailpit_expected_subject "$episode_one")" \
  "$(mailpit_expected_subject "$episode_two")" <<'PY' &
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit


test_directory = Path(sys.argv[1])
expected_subjects = [sys.argv[2], sys.argv[3]]
adversarial_ids = ['current\n"quoted-id', 'older\n"quoted-id']
messages = [
  {"ID": f"foreign-{index}", "Subject": f"foreign subject {index}"}
  for index in range(47)
]
messages.extend(
  [
    {"ID": "near-current", "Subject": f"{expected_subjects[0]} extra"},
    {"ID": "near-older", "Subject": f"re: {expected_subjects[1]}"},
    {"ID": adversarial_ids[0], "Subject": expected_subjects[0]},
    {"ID": adversarial_ids[1], "Subject": expected_subjects[1]},
  ]
)
request_log: list[dict[str, object]] = []


def persist_log() -> None:
  (test_directory / "requests.json").write_text(json.dumps(request_log), encoding="utf-8")


class Handler(BaseHTTPRequestHandler):
  def log_message(self, format: str, *arguments: object) -> None:
    return

  def send_json(self, status: int, payload: object) -> None:
    encoded = json.dumps(payload).encode("utf-8")
    self.send_response(status)
    self.send_header("Content-Type", "application/json")
    self.send_header("Content-Length", str(len(encoded)))
    self.end_headers()
    self.wfile.write(encoded)

  def do_GET(self) -> None:
    parsed = urlsplit(self.path)
    query = parse_qs(parsed.query, keep_blank_values=True)
    if parsed.path != "/api/v1/messages" or set(query) != {"start", "limit"}:
      self.send_json(400, {"error": "unexpected listing request"})
      return
    try:
      start = int(query["start"][0])
      limit = int(query["limit"][0])
    except (KeyError, ValueError, IndexError):
      self.send_json(400, {"error": "invalid pagination"})
      return
    if start < 0 or limit != 50 or len(query["start"]) != 1 or len(query["limit"]) != 1:
      self.send_json(400, {"error": "unexpected pagination"})
      return
    page = messages[start : start + limit]
    request_log.append({"method": "GET", "start": start, "limit": limit})
    persist_log()
    self.send_json(
      200,
      {
        "total": len(messages),
        "count": len(page),
        "start": start,
        "messages_count": len(messages),
        "messages_unread": 0,
        "tags": [],
        "messages": page,
      },
    )

  def do_DELETE(self) -> None:
    parsed = urlsplit(self.path)
    length = int(self.headers.get("Content-Length", "0"))
    raw_body = self.rfile.read(length)
    try:
      body = json.loads(raw_body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
      self.send_json(400, {"error": "invalid JSON body"})
      return
    if (
      parsed.path != "/api/v1/messages"
      or not isinstance(body, dict)
      or set(body) != {"IDs"}
      or not isinstance(body["IDs"], list)
      or len(body["IDs"]) != 1
      or not isinstance(body["IDs"][0], str)
      or not body["IDs"][0]
    ):
      self.send_json(400, {"error": "unsafe delete request"})
      return
    message_id = body["IDs"][0]
    request_log.append({"method": "DELETE", "body": body})
    messages[:] = [message for message in messages if message["ID"] != message_id]
    persist_log()
    self.send_response(204)
    self.end_headers()


server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
(test_directory / "port").write_text(str(server.server_address[1]), encoding="utf-8")
server.serve_forever()
PY
  server_pid="$!"

  for _ in {1..50}; do
  if [[ -s "$test_directory/port" ]]; then
    break
  fi
  if ! kill -0 "$server_pid" >/dev/null 2>&1; then
    wait "$server_pid"
    return 1
  fi
  sleep 0.1
  done
  if [[ ! -s "$test_directory/port" ]]; then
  printf '%s\n' "Mailpit helper self-test mock did not start." >&2
  return 1
  fi
  port="$(<"$test_directory/port")"
  mailpit_api="http://127.0.0.1:${port}/api/v1"

  cleanup_mailpit_messages

  python3 - "$test_directory/requests.json" <<'PY'
import json
import sys


with open(sys.argv[1], encoding="utf-8") as request_file:
  requests = json.load(request_file)

get_offsets = [request["start"] for request in requests if request["method"] == "GET"]
if 50 not in get_offsets:
  raise SystemExit("Mailpit helper self-test did not request the second page")
deletes = [request for request in requests if request["method"] == "DELETE"]
expected_ids = ['current\n"quoted-id', 'older\n"quoted-id']
if [request["body"] for request in deletes] != [{"IDs": [message_id]} for message_id in expected_ids]:
  raise SystemExit("Mailpit helper self-test observed an unsafe delete body")
last_delete = max(index for index, request in enumerate(requests) if request["method"] == "DELETE")
if not any(
  request["method"] == "GET" and request["start"] == 0
  for request in requests[last_delete + 1 :]
):
  raise SystemExit("Mailpit helper self-test did not re-query after deletion")
PY
  printf '%s\n' "Mailpit cleanup helper pagination and JSON-ID self-test passed."
)

run_cleanup_only_self_test() (
  set -euo pipefail

  local script_path="$repository_root/scripts/test-alerta-alarm-flow.sh"

  expect_cleanup_only_rejection() {
    local omitted_variable="$1"
    local exit_code=0
    local -a invocation_environment=(
      "WAMA_RUN_ROOT_ALARM_FLOW_TEST=run-root-alarm-flow-test"
      "ALERTA_ALARM_FLOW_CLEANUP_ONLY=cleanup-only"
    )

    if [[ "$omitted_variable" != "ALERTA_ALARM_FLOW_CLEANUP_CONFIRM" ]]; then
      invocation_environment+=(
        "ALERTA_ALARM_FLOW_CLEANUP_CONFIRM=delete-confirmed-mailpit-test-artifacts"
      )
    fi
    if [[ "$omitted_variable" != "ALERTA_ALARM_FLOW_RUN_ID" ]]; then
      invocation_environment+=("ALERTA_ALARM_FLOW_RUN_ID=cleanup-only-self-test")
    fi
    if [[ "$omitted_variable" != "ALERTA_ALARM_FLOW_EPISODE_ONE" ]]; then
      invocation_environment+=("ALERTA_ALARM_FLOW_EPISODE_ONE=cleanup-only-episode-one")
    fi
    if [[ "$omitted_variable" != "ALERTA_ALARM_FLOW_EPISODE_TWO" ]]; then
      invocation_environment+=("ALERTA_ALARM_FLOW_EPISODE_TWO=cleanup-only-episode-two")
    fi

    env \
      -u ALERTA_ALARM_FLOW_CLEANUP_ONLY_SELF_TEST \
      -u ALERTA_ALARM_FLOW_MAILPIT_HELPER_TEST \
      -u ALERTA_ALARM_FLOW_CLEANUP_CONFIRM \
      -u ALERTA_ALARM_FLOW_RUN_ID \
      -u ALERTA_ALARM_FLOW_EPISODE_ONE \
      -u ALERTA_ALARM_FLOW_EPISODE_TWO \
      "${invocation_environment[@]}" \
      "$script_path" >/dev/null 2>&1 || exit_code=$?
    if [[ "$exit_code" != "2" ]]; then
      printf 'Cleanup-only self-test expected %s to be rejected with exit status 2, got %s.\n' \
        "$omitted_variable" "$exit_code" >&2
      return 1
    fi
  }

  expect_cleanup_only_rejection ALERTA_ALARM_FLOW_CLEANUP_CONFIRM
  expect_cleanup_only_rejection ALERTA_ALARM_FLOW_RUN_ID
  expect_cleanup_only_rejection ALERTA_ALARM_FLOW_EPISODE_ONE
  expect_cleanup_only_rejection ALERTA_ALARM_FLOW_EPISODE_TWO
  run_mailpit_cleanup_helper_self_test
  printf '%s\n' "Cleanup-only validation and exact-subject deletion self-test passed."
)

wait_for_email_count() {
  local episode_id="$1"
  local expected_count="$2"
  local deadline=$((SECONDS + ${ALERTA_ALARM_FLOW_TIMEOUT_SECONDS:-90}))
  local actual_count

  while ((SECONDS < deadline)); do
    actual_count="$(mailpit_episode_count "$episode_id")"
    if [[ "$actual_count" == "$expected_count" ]]; then
      return 0
    fi
    sleep 1
  done

  printf 'Expected %s Mailpit messages for %s, found %s\n' \
    "$expected_count" "$episode_id" "$actual_count" >&2
  return 1
}

assert_email_count_stable() {
  local episode_id="$1"
  local expected_count="$2"
  local checks=0

  while ((checks < 4)); do
    if [[ "$(mailpit_episode_count "$episode_id")" != "$expected_count" ]]; then
      printf 'Mailpit sent an unexpected duplicate for %s\n' "$episode_id" >&2
      return 1
    fi
    checks=$((checks + 1))
    sleep 1
  done
}

acknowledge_alert() {
  local alert_id="$1"

  curl --fail --silent --show-error \
    --request PUT \
    --header 'Content-Type: application/json' \
    --data '{"status":"ack","text":"Focused WAMA alarm-flow acknowledgement"}' \
    "${alerta_api}/alert/${alert_id}/status" >/dev/null
}

create_foreign_alert() {
  curl --fail --silent --show-error \
    --request POST \
    --header "Authorization: Key ${alerta_api_key}" \
    --header 'Content-Type: application/json' \
    --data "{\"resource\":\"${foreign_resource}\",\"event\":\"${foreign_event}\",\"environment\":\"WAMA\",\"customer\":\"wama\",\"severity\":\"indeterminate\",\"service\":[\"foreign-test\"],\"text\":\"Foreign focused test alert\",\"tags\":[\"foreign-test\"],\"attributes\":{\"foreign_test\":\"true\"}}" \
    "${alerta_api}/alert" \
    | python3 -c 'import json, sys; print(json.load(sys.stdin)["id"])'
}

foreign_alert_is_open() {
  curl --fail --silent --show-error \
    --header "Authorization: Key ${alerta_api_key}" \
    "${alerta_api}/alert/${foreign_alert_id}" \
    | python3 -c 'import json, sys; raise SystemExit(0 if json.load(sys.stdin)["alert"]["status"] == "open" else 1)'
}

initialise_isolated_alarm_topic() {
  local deadline=$((SECONDS + ${ALERTA_ALARM_FLOW_TIMEOUT_SECONDS:-90}))

  while ((SECONDS < deadline)); do
    if compose exec -T kafka \
      /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server kafka:9092 \
      --create \
      --if-not-exists \
      --topic Alarm \
      --partitions 1 \
      --replication-factor 1 \
      --config cleanup.policy=compact >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  printf '%s\n' "Timed out creating the isolated compacted Alarm topic." >&2
  return 1
}

if [[ "$cleanup_only_self_test" == true ]]; then
  run_cleanup_only_self_test
  exit 0
fi

if [[ "$mailpit_helper_self_test" == true ]]; then
  run_mailpit_cleanup_helper_self_test
  exit 0
fi

if [[ "$cleanup_only" == true ]]; then
  verify_root_stack_ready
  mailpit_ui_port="$(published_host_port mailpit 8025)"
  mailpit_api="http://127.0.0.1:${mailpit_ui_port}/api/v1"
  wait_for_http "http://127.0.0.1:${mailpit_ui_port}/readyz" "Mailpit readiness endpoint"
  cleanup_mailpit_messages
  printf '%s\n' "Mailpit cleanup-only validation passed."
  exit 0
fi

trap show_failure_diagnostics ERR
trap cleanup_fixtures EXIT

if [[ "$isolated" == true ]]; then
  alerta_host_port="$ALERTA_HOST_PORT"
  mailpit_ui_port="$MAILPIT_UI_PORT"
else
  verify_root_stack_ready
  alerta_host_port="$(published_host_port alerta 8080)"
  mailpit_ui_port="$(published_host_port mailpit 8025)"
fi
alerta_api="http://127.0.0.1:${alerta_host_port}/api"
mailpit_api="http://127.0.0.1:${mailpit_ui_port}/api/v1"

compose config --quiet
if [[ "$isolated" == true ]]; then
  compose up -d --build kafka alerta-postgres mailpit
  wait_for_service_health kafka
  wait_for_service_health alerta-postgres
  wait_for_service_health mailpit
  initialise_isolated_alarm_topic
  compose up -d --build alerta
  compose start alerta
fi

wait_for_http "${alerta_api}/management/gtg" "Alerta good-to-go endpoint"
wait_for_http "http://127.0.0.1:${mailpit_ui_port}/readyz" "Mailpit readiness endpoint"

if [[ "$isolated" == true ]]; then
  compose up -d --build alarm-alerta-ingress
  compose start alarm-alerta-ingress
fi
wait_for_ingress
ingress_ready=true

publish_active "$episode_one" WARNING "test-r1" "Focused WAMA warning alarm"
episode_one_state="$(wait_for_alert_state "$episode_one" open)"
episode_one_alert_id="${episode_one_state%% *}"
wait_for_email_count "$episode_one" 1

publish_active "$episode_one" WARNING "test-r1" "Focused WAMA warning refresh"
assert_email_count_stable "$episode_one" 1

acknowledge_alert "$episode_one_alert_id"
wait_for_alert_state "$episode_one" ack >/dev/null
publish_active "$episode_one" CRITICAL "test-r2" "Focused WAMA critical revision"
wait_for_alert_state "$episode_one" ack >/dev/null
assert_email_count_stable "$episode_one" 1

publish_tombstone "$episode_one"
wait_for_alert_state "$episode_one" closed >/dev/null
assert_email_count_stable "$episode_one" 1

publish_active "$episode_two" WARNING "test-r3" "Focused WAMA new episode"
wait_for_alert_state "$episode_two" open >/dev/null
wait_for_email_count "$episode_two" 1

foreign_alert_id="$(create_foreign_alert)"
foreign_alert_is_open

compose restart alarm-alerta-ingress
wait_for_ingress
wait_for_alert_state "$episode_two" open >/dev/null
assert_email_count_stable "$episode_two" 1
foreign_alert_is_open

publish_tombstone "$episode_two"
wait_for_alert_state "$episode_two" closed >/dev/null
assert_email_count_stable "$episode_two" 1
close_foreign_alert

cleanup_fixtures verify-mailpit-cleanup
trap - ERR
trap - EXIT
printf '%s\n' "Alerta Alarm active/ack/revision/tombstone/restart flow validation passed."