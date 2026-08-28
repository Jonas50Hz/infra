#!/usr/bin/env bash

set -euo pipefail

script_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
initializer="$script_directory/../create-topics.sh"
temporary_root="$(mktemp -d)"
readonly alarm_evaluation_watermark_migration_guard="accept-forward-only-alarm-evaluation-watermark-v1"

cleanup() {
  rm -rf "$temporary_root"
}
trap cleanup EXIT

fail() {
  printf 'FAIL: %s\n' "$1" >&2
  exit 1
}

assert_contains() {
  local expected="$1"
  local file="$2"

  if ! grep -F -- "$expected" "$file" >/dev/null; then
    fail "Expected $file to contain $expected"
  fi
}

assert_not_contains() {
  local expected="$1"
  local file="$2"

  if grep -F -- "$expected" "$file" >/dev/null; then
    fail "Expected $file not to contain $expected"
  fi
}

assert_not_line() {
  local expected="$1"
  local file="$2"

  if grep -F -x -- "$expected" "$file" >/dev/null; then
    fail "Expected $file not to contain line $expected"
  fi
}

assert_line() {
  local expected="$1"
  local file="$2"

  if ! grep -F -x -- "$expected" "$file" >/dev/null; then
    fail "Expected $file to contain line $expected"
  fi
}

assert_line_before() {
  local first="$1"
  local second="$2"
  local file="$3"
  local line
  local first_seen=0

  while IFS= read -r line; do
    if [[ "$line" == "$first" ]]; then
      first_seen=1
      continue
    fi
    if [[ "$line" == "$second" ]]; then
      if (( first_seen )); then
        return
      fi
      fail "Expected line $first before $second in $file"
    fi
  done < "$file"

  fail "Expected line $first before $second in $file"
}

new_case() {
  local case_directory

  case_directory="$(mktemp -d "$temporary_root/case.XXXXXX")"
  mkdir -p "$case_directory/bin"
  printf '%s\n' "$case_directory"
}

write_alarm_state() {
  local case_directory="$1"
  local present="$2"
  local partitions="$3"
  local replication_factor="$4"
  local cleanup_policy="$5"
  local end_offset="$6"
  local fail_list_after_delete="${7:-0}"
  local watermark_present="${8:-0}"
  local watermark_partitions="${9:-1}"
  local watermark_replication_factor="${10:-1}"
  local watermark_cleanup_policy="${11:-compact}"
  local alarm_topic_id="${12:-original-alarm-topic-id}"
  local alarm_retained_bytes="${13:-original-alarm-retained-bytes}"

  cat > "$case_directory/state" <<EOF
present=$present
partitions=$partitions
replication_factor=$replication_factor
cleanup_policy=$cleanup_policy
end_offset=$end_offset
fail_list_after_delete=$fail_list_after_delete
watermark_present=$watermark_present
watermark_partitions=$watermark_partitions
watermark_replication_factor=$watermark_replication_factor
watermark_cleanup_policy=$watermark_cleanup_policy
alarm_topic_id=$alarm_topic_id
alarm_retained_bytes=$alarm_retained_bytes
EOF
}

state_value() {
  local case_directory="$1"
  local name="$2"

  grep -E "^${name}=" "$case_directory/state" | cut -d= -f2-
}

assert_state_value() {
  local case_directory="$1"
  local name="$2"
  local expected="$3"
  local actual

  actual="$(state_value "$case_directory" "$name")"
  if [[ "$actual" != "$expected" ]]; then
    fail "Expected state $name=$expected, found $actual"
  fi
}

write_mock_commands() {
  local case_directory="$1"
  local mock_bin="$case_directory/bin"

  cat > "$mock_bin/kafka-topics.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

source "$MOCK_KAFKA_STATE"

argument_value() {
  local expected="$1"
  shift

  while (( $# )); do
    if [[ "$1" == "$expected" ]]; then
      printf '%s\n' "$2"
      return 0
    fi
    shift
  done

  return 1
}

has_argument() {
  local expected="$1"
  shift
  local argument

  for argument in "$@"; do
    if [[ "$argument" == "$expected" ]]; then
      return 0
    fi
  done

  return 1
}

save_state() {
  printf 'present=%s\npartitions=%s\nreplication_factor=%s\ncleanup_policy=%s\nend_offset=%s\nfail_list_after_delete=%s\nwatermark_present=%s\nwatermark_partitions=%s\nwatermark_replication_factor=%s\nwatermark_cleanup_policy=%s\nalarm_topic_id=%s\nalarm_retained_bytes=%s\n' \
    "$present" \
    "$partitions" \
    "$replication_factor" \
    "$cleanup_policy" \
    "$end_offset" \
    "$fail_list_after_delete" \
    "$watermark_present" \
    "$watermark_partitions" \
    "$watermark_replication_factor" \
    "$watermark_cleanup_policy" \
    "$alarm_topic_id" \
    "$alarm_retained_bytes" > "$MOCK_KAFKA_STATE"
}

topic_partitions() {
  case "$1" in
    MeasurementSession|Blobmeta)
      printf '%s\n' "12"
      ;;
    *)
      printf '%s\n' "1"
      ;;
  esac
}

topic="$(argument_value --topic "$@" || true)"

if has_argument --list "$@"; then
  if [[ "$fail_list_after_delete" == "1" && "$present" == "0" ]]; then
    printf '%s\n' "topics:list:failed" >> "$MOCK_KAFKA_LOG"
    printf '%s\n' "Kafka topic listing failed" >&2
    exit 1
  fi
  printf '%s\n' "LiveMeasurement" "MeasurementSession" "Export" "Masterdata" "Schema" "Blobmeta"
  if [[ "$present" == "1" ]]; then
    printf '%s\n' "Alarm"
  fi
  if [[ "$watermark_present" == "1" ]]; then
    printf '%s\n' "AlarmEvaluationWatermark"
  fi
  printf '%s\n' "topics:list" >> "$MOCK_KAFKA_LOG"
  exit 0
fi

if has_argument --describe "$@"; then
  case "$topic" in
    Alarm)
      if [[ "$present" != "1" ]]; then
        printf 'Alarm is absent\n' >&2
        exit 1
      fi
      printf 'Topic: Alarm PartitionCount: %s ReplicationFactor: %s Configs:\n' \
        "$partitions" \
        "$replication_factor"
      ;;
    AlarmEvaluationWatermark)
      if [[ "$watermark_present" != "1" ]]; then
        printf 'AlarmEvaluationWatermark is absent\n' >&2
        exit 1
      fi
      printf 'Topic: AlarmEvaluationWatermark PartitionCount: %s ReplicationFactor: %s Configs:\n' \
        "$watermark_partitions" \
        "$watermark_replication_factor"
      ;;
    *)
      printf 'Topic: %s PartitionCount: %s ReplicationFactor: 1 Configs:\n' \
        "$topic" \
        "$(topic_partitions "$topic")"
      ;;
  esac
  printf 'topics:describe:%s\n' "$topic" >> "$MOCK_KAFKA_LOG"
  exit 0
fi

if has_argument --create "$@"; then
  printf 'topics:create:%s\n' "$topic" >> "$MOCK_KAFKA_LOG"
  if [[ "$topic" == "Alarm" ]]; then
    present=1
    partitions="$(argument_value --partitions "$@")"
    replication_factor="$(argument_value --replication-factor "$@")"
    cleanup_policy="delete"
    end_offset=0
    alarm_topic_id="created-alarm-topic-id"
    alarm_retained_bytes=""
    save_state
  elif [[ "$topic" == "AlarmEvaluationWatermark" ]]; then
    watermark_present=1
    watermark_partitions="$(argument_value --partitions "$@")"
    watermark_replication_factor="$(argument_value --replication-factor "$@")"
    watermark_cleanup_policy="delete"
    save_state
  fi
  exit 0
fi

if has_argument --delete "$@"; then
  printf 'topics:delete:%s\n' "$topic" >> "$MOCK_KAFKA_LOG"
  if [[ "$topic" != "Alarm" ]]; then
    printf 'Unexpected deletion request for %s\n' "$topic" >&2
    exit 1
  fi
  present=0
  alarm_topic_id=""
  alarm_retained_bytes=""
  save_state
  exit 0
fi

fail "Unexpected kafka-topics invocation: $*"
EOF

  cat > "$mock_bin/kafka-configs.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

source "$MOCK_KAFKA_STATE"

argument_value() {
  local expected="$1"
  shift

  while (( $# )); do
    if [[ "$1" == "$expected" ]]; then
      printf '%s\n' "$2"
      return 0
    fi
    shift
  done

  return 1
}

has_argument() {
  local expected="$1"
  shift
  local argument

  for argument in "$@"; do
    if [[ "$argument" == "$expected" ]]; then
      return 0
    fi
  done

  return 1
}

save_state() {
  printf 'present=%s\npartitions=%s\nreplication_factor=%s\ncleanup_policy=%s\nend_offset=%s\nfail_list_after_delete=%s\nwatermark_present=%s\nwatermark_partitions=%s\nwatermark_replication_factor=%s\nwatermark_cleanup_policy=%s\nalarm_topic_id=%s\nalarm_retained_bytes=%s\n' \
    "$present" \
    "$partitions" \
    "$replication_factor" \
    "$cleanup_policy" \
    "$end_offset" \
    "$fail_list_after_delete" \
    "$watermark_present" \
    "$watermark_partitions" \
    "$watermark_replication_factor" \
    "$watermark_cleanup_policy" \
    "$alarm_topic_id" \
    "$alarm_retained_bytes" > "$MOCK_KAFKA_STATE"
}

topic="$(argument_value --entity-name "$@" || true)"

if has_argument --describe "$@"; then
  case "$topic" in
    Alarm)
      policy="$cleanup_policy"
      ;;
    AlarmEvaluationWatermark)
      policy="$watermark_cleanup_policy"
      ;;
    LiveMeasurement|MeasurementSession|Export)
      policy="delete"
      ;;
    *)
      policy="compact"
      ;;
  esac
  printf 'Dynamic configs for topic %s are cleanup.policy=%s sensitive=false\n' \
    "$topic" \
    "$policy"
  printf 'configs:describe:%s\n' "$topic" >> "$MOCK_KAFKA_LOG"
  exit 0
fi

if has_argument --alter "$@"; then
  config="$(argument_value --add-config "$@")"
  printf 'configs:alter:%s:%s\n' "$topic" "$config" >> "$MOCK_KAFKA_LOG"
  if [[ "$topic" == "Alarm" ]]; then
    cleanup_policy="${config#cleanup.policy=}"
    save_state
  elif [[ "$topic" == "AlarmEvaluationWatermark" ]]; then
    watermark_cleanup_policy="${config#cleanup.policy=}"
    save_state
  fi
  exit 0
fi

printf 'Unexpected kafka-configs invocation: %s\n' "$*" >&2
exit 1
EOF

  cat > "$mock_bin/kafka-get-offsets.sh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

source "$MOCK_KAFKA_STATE"

if [[ "$present" != "1" ]]; then
  printf 'Alarm is absent\n' >&2
  exit 1
fi

printf 'Alarm:0:%s\n' "$end_offset"
printf 'offsets:Alarm\n' >> "$MOCK_KAFKA_LOG"
EOF

  chmod +x \
    "$mock_bin/kafka-topics.sh" \
    "$mock_bin/kafka-configs.sh" \
    "$mock_bin/kafka-get-offsets.sh"
}

run_initializer() {
  local case_directory="$1"
  local alarm_migration_guard="$2"
  local watermark_migration_guard="${3:-}"

  env \
    MOCK_KAFKA_STATE="$case_directory/state" \
    MOCK_KAFKA_LOG="$case_directory/commands.log" \
    KAFKA_TOPICS_COMMAND="$case_directory/bin/kafka-topics.sh" \
    KAFKA_CONFIGS_COMMAND="$case_directory/bin/kafka-configs.sh" \
    KAFKA_GET_OFFSETS_COMMAND="$case_directory/bin/kafka-get-offsets.sh" \
    WAMA_ALARM_LEGACY_MIGRATION="$alarm_migration_guard" \
    WAMA_ALARM_EVALUATION_WATERMARK_MIGRATION="$watermark_migration_guard" \
    bash "$initializer" > "$case_directory/output.log" 2>&1
}

expect_initializer_success() {
  local case_directory="$1"
  local alarm_migration_guard="$2"
  local watermark_migration_guard="${3:-}"

  if ! run_initializer "$case_directory" "$alarm_migration_guard" "$watermark_migration_guard"; then
    cat "$case_directory/output.log" >&2
    fail "Initializer unexpectedly failed"
  fi
}

expect_initializer_failure() {
  local case_directory="$1"
  local alarm_migration_guard="$2"
  local watermark_migration_guard="${3:-}"

  if run_initializer "$case_directory" "$alarm_migration_guard" "$watermark_migration_guard"; then
    cat "$case_directory/state" >&2
    cat "$case_directory/commands.log" >&2
    cat "$case_directory/output.log" >&2
    fail "Initializer unexpectedly succeeded"
  fi
}

test_creates_absent_compacted_alarm_topic() {
  local case_directory

  case_directory="$(new_case)"
  write_alarm_state "$case_directory" 0 1 1 delete 0
  write_mock_commands "$case_directory"

  expect_initializer_success "$case_directory" ""

  assert_state_value "$case_directory" present 1
  assert_state_value "$case_directory" partitions 1
  assert_state_value "$case_directory" replication_factor 1
  assert_state_value "$case_directory" cleanup_policy compact
  assert_state_value "$case_directory" watermark_present 1
  assert_state_value "$case_directory" watermark_partitions 1
  assert_state_value "$case_directory" watermark_replication_factor 1
  assert_state_value "$case_directory" watermark_cleanup_policy compact
  assert_line "topics:create:Alarm" "$case_directory/commands.log"
  assert_line "topics:create:AlarmEvaluationWatermark" "$case_directory/commands.log"
  assert_contains "configs:alter:Alarm:cleanup.policy=compact" "$case_directory/commands.log"
}

test_migrates_empty_legacy_alarm_topic_in_place() {
  local case_directory

  case_directory="$(new_case)"
  write_alarm_state "$case_directory" 1 1 1 delete 0
  write_mock_commands "$case_directory"

  expect_initializer_success "$case_directory" ""

  assert_state_value "$case_directory" cleanup_policy compact
  assert_state_value "$case_directory" watermark_present 1
  assert_state_value "$case_directory" watermark_cleanup_policy compact
  assert_contains "offsets:Alarm" "$case_directory/commands.log"
  assert_line "topics:create:AlarmEvaluationWatermark" "$case_directory/commands.log"
  assert_contains "configs:alter:Alarm:cleanup.policy=compact" "$case_directory/commands.log"
  assert_not_contains "topics:delete:Alarm" "$case_directory/commands.log"
  assert_not_line "topics:create:Alarm" "$case_directory/commands.log"
}

test_creates_watermark_when_alarm_is_absent_with_exact_guard() {
  local case_directory

  case_directory="$(new_case)"
  write_alarm_state "$case_directory" 0 1 1 delete 0
  write_mock_commands "$case_directory"

  expect_initializer_success "$case_directory" "" "$alarm_evaluation_watermark_migration_guard"

  assert_state_value "$case_directory" watermark_present 1
  assert_state_value "$case_directory" watermark_partitions 1
  assert_state_value "$case_directory" watermark_replication_factor 1
  assert_state_value "$case_directory" watermark_cleanup_policy compact
  assert_line "topics:create:AlarmEvaluationWatermark" "$case_directory/commands.log"
}

test_creates_watermark_when_alarm_is_empty_with_exact_guard() {
  local case_directory

  case_directory="$(new_case)"
  write_alarm_state "$case_directory" 1 1 1 compact 0
  write_mock_commands "$case_directory"

  expect_initializer_success "$case_directory" "" "$alarm_evaluation_watermark_migration_guard"

  assert_state_value "$case_directory" watermark_present 1
  assert_state_value "$case_directory" watermark_cleanup_policy compact
  assert_line "topics:create:AlarmEvaluationWatermark" "$case_directory/commands.log"
  assert_not_line "topics:create:Alarm" "$case_directory/commands.log"
  assert_not_line "topics:delete:Alarm" "$case_directory/commands.log"
  assert_not_line "configs:alter:Alarm:cleanup.policy=compact" "$case_directory/commands.log"
}

test_rejects_nonempty_alarm_without_watermark_guard() {
  local case_directory

  case_directory="$(new_case)"
  write_alarm_state "$case_directory" 1 1 1 compact 9
  write_mock_commands "$case_directory"

  expect_initializer_failure "$case_directory" ""

  assert_contains "WAMA_ALARM_EVALUATION_WATERMARK_MIGRATION=$alarm_evaluation_watermark_migration_guard" "$case_directory/output.log"
  assert_state_value "$case_directory" watermark_present 0
  assert_not_line "topics:create:AlarmEvaluationWatermark" "$case_directory/commands.log"
  assert_not_line "topics:create:Alarm" "$case_directory/commands.log"
  assert_not_line "topics:delete:Alarm" "$case_directory/commands.log"
  assert_not_line "configs:alter:Alarm:cleanup.policy=compact" "$case_directory/commands.log"
}

test_rejects_nonempty_alarm_with_invalid_watermark_guard() {
  local case_directory

  case_directory="$(new_case)"
  write_alarm_state "$case_directory" 1 1 1 compact 9
  write_mock_commands "$case_directory"

  expect_initializer_failure "$case_directory" "" "accept-forward-only-alarm-evaluation-watermark-v2"

  assert_contains "WAMA_ALARM_EVALUATION_WATERMARK_MIGRATION=$alarm_evaluation_watermark_migration_guard" "$case_directory/output.log"
  assert_state_value "$case_directory" watermark_present 0
  assert_not_line "topics:create:AlarmEvaluationWatermark" "$case_directory/commands.log"
  assert_not_line "topics:create:Alarm" "$case_directory/commands.log"
  assert_not_line "topics:delete:Alarm" "$case_directory/commands.log"
  assert_not_line "configs:alter:Alarm:cleanup.policy=compact" "$case_directory/commands.log"
}

test_creates_watermark_for_nonempty_alarm_with_exact_guard_without_alarm_mutation() {
  local case_directory

  case_directory="$(new_case)"
  write_alarm_state "$case_directory" 1 1 1 compact 9
  write_mock_commands "$case_directory"

  expect_initializer_success "$case_directory" "" "$alarm_evaluation_watermark_migration_guard"

  assert_state_value "$case_directory" present 1
  assert_state_value "$case_directory" cleanup_policy compact
  assert_state_value "$case_directory" end_offset 9
  assert_state_value "$case_directory" alarm_topic_id original-alarm-topic-id
  assert_state_value "$case_directory" alarm_retained_bytes original-alarm-retained-bytes
  assert_state_value "$case_directory" watermark_present 1
  assert_state_value "$case_directory" watermark_partitions 1
  assert_state_value "$case_directory" watermark_replication_factor 1
  assert_state_value "$case_directory" watermark_cleanup_policy compact
  assert_line "topics:create:AlarmEvaluationWatermark" "$case_directory/commands.log"
  assert_line "configs:alter:AlarmEvaluationWatermark:cleanup.policy=compact" "$case_directory/commands.log"
  assert_not_line "topics:create:Alarm" "$case_directory/commands.log"
  assert_not_line "topics:delete:Alarm" "$case_directory/commands.log"
  assert_not_line "configs:alter:Alarm:cleanup.policy=compact" "$case_directory/commands.log"
}

test_accepts_existing_well_formed_watermark_without_mutation() {
  local case_directory

  case_directory="$(new_case)"
  write_alarm_state "$case_directory" 1 1 1 compact 9 0 1 1 1 compact
  write_mock_commands "$case_directory"

  expect_initializer_success "$case_directory" ""

  assert_not_line "topics:create:AlarmEvaluationWatermark" "$case_directory/commands.log"
  assert_not_line "configs:alter:AlarmEvaluationWatermark:cleanup.policy=compact" "$case_directory/commands.log"
  assert_not_line "topics:create:Alarm" "$case_directory/commands.log"
  assert_not_line "topics:delete:Alarm" "$case_directory/commands.log"
  assert_not_line "configs:alter:Alarm:cleanup.policy=compact" "$case_directory/commands.log"
}

test_rejects_malformed_watermark_without_mutation() {
  local topology_case
  local cleanup_policy_case

  topology_case="$(new_case)"
  write_alarm_state "$topology_case" 1 1 1 compact 9 0 1 2 1 compact
  write_mock_commands "$topology_case"

  expect_initializer_failure "$topology_case" ""

  assert_contains "AlarmEvaluationWatermark must have 1 partitions" "$topology_case/output.log"
  assert_not_line "configs:alter:AlarmEvaluationWatermark:cleanup.policy=compact" "$topology_case/commands.log"
  assert_not_line "topics:create:Alarm" "$topology_case/commands.log"
  assert_not_line "topics:delete:Alarm" "$topology_case/commands.log"
  assert_not_line "configs:alter:Alarm:cleanup.policy=compact" "$topology_case/commands.log"

  cleanup_policy_case="$(new_case)"
  write_alarm_state "$cleanup_policy_case" 1 1 1 compact 9 0 1 1 1 compact,delete
  write_mock_commands "$cleanup_policy_case"

  expect_initializer_failure "$cleanup_policy_case" ""

  assert_contains "AlarmEvaluationWatermark must use cleanup.policy=compact" "$cleanup_policy_case/output.log"
  assert_not_line "configs:alter:AlarmEvaluationWatermark:cleanup.policy=compact" "$cleanup_policy_case/commands.log"
  assert_not_line "topics:create:Alarm" "$cleanup_policy_case/commands.log"
  assert_not_line "topics:delete:Alarm" "$cleanup_policy_case/commands.log"
  assert_not_line "configs:alter:Alarm:cleanup.policy=compact" "$cleanup_policy_case/commands.log"
}

test_rejects_nonempty_legacy_alarm_without_guard() {
  local case_directory

  case_directory="$(new_case)"
  write_alarm_state "$case_directory" 1 1 1 delete 9
  write_mock_commands "$case_directory"

  expect_initializer_failure "$case_directory" "" "$alarm_evaluation_watermark_migration_guard"

  assert_contains "retains records and will not be deleted" "$case_directory/output.log"
  assert_state_value "$case_directory" present 1
  assert_state_value "$case_directory" cleanup_policy delete
  assert_state_value "$case_directory" end_offset 9
  assert_state_value "$case_directory" alarm_topic_id original-alarm-topic-id
  assert_state_value "$case_directory" alarm_retained_bytes original-alarm-retained-bytes
  assert_state_value "$case_directory" watermark_present 0
  assert_not_contains "topics:delete:Alarm" "$case_directory/commands.log"
  assert_not_line "topics:create:Alarm" "$case_directory/commands.log"
  assert_not_line "configs:alter:Alarm:cleanup.policy=compact" "$case_directory/commands.log"
  assert_not_line "topics:create:AlarmEvaluationWatermark" "$case_directory/commands.log"
  assert_not_line "configs:alter:AlarmEvaluationWatermark:cleanup.policy=compact" "$case_directory/commands.log"
}

test_rejects_nonempty_legacy_alarm_with_invalid_guard() {
  local case_directory

  case_directory="$(new_case)"
  write_alarm_state "$case_directory" 1 1 1 delete 9
  write_mock_commands "$case_directory"

  expect_initializer_failure "$case_directory" "discard-delete-retained-alarm-v2" "$alarm_evaluation_watermark_migration_guard"

  assert_contains "WAMA_ALARM_LEGACY_MIGRATION=discard-delete-retained-alarm-v1" "$case_directory/output.log"
  assert_state_value "$case_directory" present 1
  assert_state_value "$case_directory" cleanup_policy delete
  assert_state_value "$case_directory" end_offset 9
  assert_state_value "$case_directory" alarm_topic_id original-alarm-topic-id
  assert_state_value "$case_directory" alarm_retained_bytes original-alarm-retained-bytes
  assert_state_value "$case_directory" watermark_present 0
  assert_not_contains "topics:delete:Alarm" "$case_directory/commands.log"
  assert_not_line "topics:create:Alarm" "$case_directory/commands.log"
  assert_not_line "configs:alter:Alarm:cleanup.policy=compact" "$case_directory/commands.log"
  assert_not_line "topics:create:AlarmEvaluationWatermark" "$case_directory/commands.log"
  assert_not_line "configs:alter:AlarmEvaluationWatermark:cleanup.policy=compact" "$case_directory/commands.log"
}

test_recreates_nonempty_legacy_alarm_with_exact_guard() {
  local case_directory

  case_directory="$(new_case)"
  write_alarm_state "$case_directory" 1 1 1 delete 9
  write_mock_commands "$case_directory"

  expect_initializer_success "$case_directory" "discard-delete-retained-alarm-v1"

  assert_state_value "$case_directory" present 1
  assert_state_value "$case_directory" cleanup_policy compact
  assert_state_value "$case_directory" end_offset 0
  assert_state_value "$case_directory" alarm_topic_id created-alarm-topic-id
  assert_state_value "$case_directory" alarm_retained_bytes ""
  assert_state_value "$case_directory" watermark_present 1
  assert_state_value "$case_directory" watermark_cleanup_policy compact
  assert_contains "topics:delete:Alarm" "$case_directory/commands.log"
  assert_contains "topics:create:Alarm" "$case_directory/commands.log"
  assert_contains "configs:alter:Alarm:cleanup.policy=compact" "$case_directory/commands.log"
  assert_line "topics:create:AlarmEvaluationWatermark" "$case_directory/commands.log"
  assert_line "configs:alter:AlarmEvaluationWatermark:cleanup.policy=compact" "$case_directory/commands.log"
  assert_line_before \
    "topics:create:Alarm" \
    "topics:create:AlarmEvaluationWatermark" \
    "$case_directory/commands.log"
}

test_does_not_recreate_alarm_when_post_delete_list_fails() {
  local case_directory

  case_directory="$(new_case)"
  write_alarm_state "$case_directory" 1 1 1 delete 9 1
  write_mock_commands "$case_directory"

  expect_initializer_failure "$case_directory" "discard-delete-retained-alarm-v1" "$alarm_evaluation_watermark_migration_guard"

  assert_state_value "$case_directory" present 0
  assert_contains "topics:delete:Alarm" "$case_directory/commands.log"
  assert_contains "topics:list:failed" "$case_directory/commands.log"
  assert_not_line "topics:create:Alarm" "$case_directory/commands.log"
  assert_contains "Unable to list Kafka topics while checking Alarm." "$case_directory/output.log"
}

test_accepts_existing_compacted_alarm_topic_without_mutation() {
  local case_directory

  case_directory="$(new_case)"
  write_alarm_state "$case_directory" 1 1 1 compact 4
  write_mock_commands "$case_directory"

  expect_initializer_success "$case_directory" "discard-delete-retained-alarm-v1" "$alarm_evaluation_watermark_migration_guard"

  assert_not_contains "topics:delete:Alarm" "$case_directory/commands.log"
  assert_not_line "topics:create:Alarm" "$case_directory/commands.log"
  assert_not_contains "configs:alter:Alarm:cleanup.policy=compact" "$case_directory/commands.log"
}

test_rejects_unexpected_alarm_topology_or_policy() {
  local topology_case
  local policy_case

  topology_case="$(new_case)"
  write_alarm_state "$topology_case" 1 2 1 delete 9
  write_mock_commands "$topology_case"

  expect_initializer_failure "$topology_case" "discard-delete-retained-alarm-v1" "$alarm_evaluation_watermark_migration_guard"

  assert_contains "must have 1 partitions" "$topology_case/output.log"
  assert_state_value "$topology_case" watermark_present 0
  assert_not_contains "topics:delete:Alarm" "$topology_case/commands.log"
  assert_not_line "topics:create:AlarmEvaluationWatermark" "$topology_case/commands.log"
  assert_not_line "configs:alter:AlarmEvaluationWatermark:cleanup.policy=compact" "$topology_case/commands.log"

  policy_case="$(new_case)"
  write_alarm_state "$policy_case" 1 1 1 compact,delete 9
  write_mock_commands "$policy_case"

  expect_initializer_failure "$policy_case" "discard-delete-retained-alarm-v1" "$alarm_evaluation_watermark_migration_guard"

  assert_contains "cleanup.policy=compact or legacy cleanup.policy=delete exactly" "$policy_case/output.log"
  assert_state_value "$policy_case" watermark_present 0
  assert_not_contains "topics:delete:Alarm" "$policy_case/commands.log"
  assert_not_line "topics:create:AlarmEvaluationWatermark" "$policy_case/commands.log"
  assert_not_line "configs:alter:AlarmEvaluationWatermark:cleanup.policy=compact" "$policy_case/commands.log"
}

test_creates_absent_compacted_alarm_topic
test_migrates_empty_legacy_alarm_topic_in_place
test_creates_watermark_when_alarm_is_absent_with_exact_guard
test_creates_watermark_when_alarm_is_empty_with_exact_guard
test_rejects_nonempty_alarm_without_watermark_guard
test_rejects_nonempty_alarm_with_invalid_watermark_guard
test_creates_watermark_for_nonempty_alarm_with_exact_guard_without_alarm_mutation
test_accepts_existing_well_formed_watermark_without_mutation
test_rejects_malformed_watermark_without_mutation
test_rejects_nonempty_legacy_alarm_without_guard
test_rejects_nonempty_legacy_alarm_with_invalid_guard
test_recreates_nonempty_legacy_alarm_with_exact_guard
test_does_not_recreate_alarm_when_post_delete_list_fails
test_accepts_existing_compacted_alarm_topic_without_mutation
test_rejects_unexpected_alarm_topology_or_policy

printf 'Kafka topic initializer migration tests passed.\n'