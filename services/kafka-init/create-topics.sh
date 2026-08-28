#!/usr/bin/env bash

set -euo pipefail

bootstrap_servers="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
kafka_topics="${KAFKA_TOPICS_COMMAND:-/opt/kafka/bin/kafka-topics.sh}"
kafka_configs="${KAFKA_CONFIGS_COMMAND:-/opt/kafka/bin/kafka-configs.sh}"
kafka_get_offsets="${KAFKA_GET_OFFSETS_COMMAND:-/opt/kafka/bin/kafka-get-offsets.sh}"
measurement_session_partitions="${MEASUREMENT_SESSION_TOPIC_PARTITIONS:-12}"
blobmeta_partitions="${BLOBMETA_TOPIC_PARTITIONS:-12}"
alarm_legacy_migration="${WAMA_ALARM_LEGACY_MIGRATION:-}"
alarm_evaluation_watermark_migration="${WAMA_ALARM_EVALUATION_WATERMARK_MIGRATION:-}"

readonly alarm_topic="Alarm"
readonly alarm_evaluation_watermark_topic="AlarmEvaluationWatermark"
readonly alarm_legacy_migration_guard="discard-delete-retained-alarm-v1"
readonly alarm_evaluation_watermark_migration_guard="accept-forward-only-alarm-evaluation-watermark-v1"
readonly alarm_topic_delete_poll_attempts=30

validate_positive_integer() {
  local name="$1"
  local value="$2"

  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    printf '%s must be a positive integer; found %s\n' "$name" "$value" >&2
    return 1
  fi
}

topic_partitions() {
  local topic="$1"

  case "$topic" in
    MeasurementSession)
      printf '%s\n' "$measurement_session_partitions"
      ;;
    Blobmeta)
      printf '%s\n' "$blobmeta_partitions"
      ;;
    *)
      printf '%s\n' "1"
      ;;
  esac
}

create_topic() {
  local topic="$1"
  local partitions

  partitions="$(topic_partitions "$topic")"

  "$kafka_topics" \
    --bootstrap-server "$bootstrap_servers" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1
}

set_cleanup_policy() {
  local topic="$1"
  local cleanup_policy="$2"

  "$kafka_configs" \
    --bootstrap-server "$bootstrap_servers" \
    --alter \
    --entity-type topics \
    --entity-name "$topic" \
    --add-config "cleanup.policy=$cleanup_policy"
}

verify_topic_layout() {
  local topic="$1"
  local description
  local expected_partitions
  local partition_pattern

  expected_partitions="$(topic_partitions "$topic")"
  partition_pattern="PartitionCount:[[:space:]]${expected_partitions}[[:space:]]"

  description=$(
    "$kafka_topics" \
      --bootstrap-server "$bootstrap_servers" \
      --describe \
      --topic "$topic"
  )

  if [[ ! "$description" =~ $partition_pattern ]] ||
    [[ ! "$description" =~ ReplicationFactor:[[:space:]]1[[:space:]] ]]; then
    printf 'Topic %s must have %s partitions and replication factor one. Current configuration:\n%s\n' \
      "$topic" "$expected_partitions" \
      "$description" >&2
    return 1
  fi
}

verify_cleanup_policy() {
  local topic="$1"
  local cleanup_policy="$2"
  local configuration
  local cleanup_policy_pattern

  configuration=$(
    "$kafka_configs" \
      --bootstrap-server "$bootstrap_servers" \
      --describe \
      --entity-type topics \
      --entity-name "$topic"
  )

  cleanup_policy_pattern="cleanup\\.policy=${cleanup_policy}[[:space:]]"
  if [[ ! "$configuration" =~ $cleanup_policy_pattern ]]; then
    printf 'Topic %s must use cleanup.policy=%s. Current configuration:\n%s\n' \
      "$topic" \
      "$cleanup_policy" \
      "$configuration" >&2
    return 1
  fi
}

topic_exists() {
  local topic="$1"
  local topic_list
  local listed_topic

  if ! topic_list=$("$kafka_topics" \
    --bootstrap-server "$bootstrap_servers" \
    --list \
    --exclude-internal); then
    printf 'Unable to list Kafka topics while checking %s.\n' "$topic" >&2
    return 2
  fi

  while IFS= read -r listed_topic; do
    if [[ "$listed_topic" == "$topic" ]]; then
      return 0
    fi
  done <<< "$topic_list"

  return 1
}

alarm_cleanup_policy() {
  local configuration
  local compact_pattern
  local delete_pattern

  configuration=$("$kafka_configs" \
    --bootstrap-server "$bootstrap_servers" \
    --describe \
    --entity-type topics \
    --entity-name "$alarm_topic")
  compact_pattern='(^|[[:space:]])cleanup\.policy=compact([[:space:]]|$)'
  delete_pattern='(^|[[:space:]])cleanup\.policy=delete([[:space:]]|$)'

  if [[ "$configuration" =~ $compact_pattern ]]; then
    printf '%s\n' "compact"
    return 0
  fi
  if [[ "$configuration" =~ $delete_pattern ]]; then
    printf '%s\n' "delete"
    return 0
  fi

  printf 'Topic %s must use cleanup.policy=compact or legacy cleanup.policy=delete exactly. Current configuration:\n%s\n' \
    "$alarm_topic" \
    "$configuration" >&2
  return 1
}

topic_end_offset() {
  local topic="$1"
  local offsets
  local offset_topic
  local partition
  local parsed_end_offset
  local retained_end_offset=""
  local extra
  local offset_count=0

  if ! offsets=$("$kafka_get_offsets" \
    --bootstrap-server "$bootstrap_servers" \
    --topic "$topic" \
    --time latest); then
    printf 'Unable to read retained offsets for topic %s.\n' "$topic" >&2
    return 1
  fi

  while IFS=: read -r offset_topic partition parsed_end_offset extra; do
    if [[ -z "$offset_topic" ]]; then
      continue
    fi
    if [[ "$offset_topic" != "$topic" ]] ||
      [[ "$partition" != "0" ]] ||
      [[ -n "$extra" ]] ||
      [[ ! "$parsed_end_offset" =~ ^[0-9]+$ ]]; then
      printf 'Topic %s returned an unexpected retained-offset layout: %s\n' \
        "$topic" \
        "$offsets" >&2
      return 1
    fi
    ((offset_count += 1))
    retained_end_offset="$parsed_end_offset"
    if (( offset_count > 1 )); then
      printf 'Topic %s returned more than one retained offset: %s\n' \
        "$topic" \
        "$offsets" >&2
      return 1
    fi
  done <<< "$offsets"

  if (( offset_count != 1 )); then
    printf 'Topic %s returned no retained offset.\n' "$topic" >&2
    return 1
  fi

  printf '%s\n' "$retained_end_offset"
}

wait_for_alarm_topic_deletion() {
  local attempt
  local exists_status

  for ((attempt = 1; attempt <= alarm_topic_delete_poll_attempts; attempt += 1)); do
    if topic_exists "$alarm_topic"; then
      if (( attempt < alarm_topic_delete_poll_attempts )); then
        sleep 1
      fi
      continue
    else
      exists_status=$?
    fi

    if (( exists_status == 1 )); then
      return 0
    fi
    return "$exists_status"
  done

  printf 'Timed out waiting for topic %s to disappear after deletion.\n' "$alarm_topic" >&2
  return 1
}

initialize_compacted_alarm_topic() {
  create_topic "$alarm_topic"
  set_cleanup_policy "$alarm_topic" "compact"
  verify_topic_layout "$alarm_topic"
  verify_cleanup_policy "$alarm_topic" "compact"
}

initialize_compacted_alarm_evaluation_watermark_topic() {
  create_topic "$alarm_evaluation_watermark_topic"
  set_cleanup_policy "$alarm_evaluation_watermark_topic" "compact"
  verify_topic_layout "$alarm_evaluation_watermark_topic"
  verify_cleanup_policy "$alarm_evaluation_watermark_topic" "compact"
}

reconcile_alarm_evaluation_watermark_topic() {
  local exists_status
  local alarm_exists_status
  local end_offset

  if topic_exists "$alarm_evaluation_watermark_topic"; then
    verify_topic_layout "$alarm_evaluation_watermark_topic"
    verify_cleanup_policy "$alarm_evaluation_watermark_topic" "compact"
    return
  else
    exists_status=$?
  fi

  if (( exists_status != 1 )); then
    return "$exists_status"
  fi

  if topic_exists "$alarm_topic"; then
    end_offset=$(topic_end_offset "$alarm_topic")
    if (( end_offset > 0 )) &&
      [[ "$alarm_evaluation_watermark_migration" != "$alarm_evaluation_watermark_migration_guard" ]]; then
      printf 'Topic %s retains records. Set WAMA_ALARM_EVALUATION_WATERMARK_MIGRATION=%s to create only %s without altering %s.\n' \
        "$alarm_topic" \
        "$alarm_evaluation_watermark_migration_guard" \
        "$alarm_evaluation_watermark_topic" \
        "$alarm_topic" >&2
      return 1
    fi
  else
    alarm_exists_status=$?
    if (( alarm_exists_status != 1 )); then
      return "$alarm_exists_status"
    fi
  fi

  initialize_compacted_alarm_evaluation_watermark_topic
}

reconcile_alarm_topic() {
  local exists_status
  local cleanup_policy
  local end_offset

  if topic_exists "$alarm_topic"; then
    verify_topic_layout "$alarm_topic"
    cleanup_policy=$(alarm_cleanup_policy)
  else
    exists_status=$?
    if (( exists_status != 1 )); then
      return "$exists_status"
    fi
    initialize_compacted_alarm_topic
    return
  fi

  case "$cleanup_policy" in
    compact)
      verify_cleanup_policy "$alarm_topic" "compact"
      return
      ;;
    delete)
      end_offset=$(topic_end_offset "$alarm_topic")
      if (( end_offset == 0 )); then
        set_cleanup_policy "$alarm_topic" "compact"
        verify_cleanup_policy "$alarm_topic" "compact"
        return
      fi

      if [[ "$alarm_legacy_migration" != "$alarm_legacy_migration_guard" ]]; then
        printf 'Topic %s retains records and will not be deleted. Set WAMA_ALARM_LEGACY_MIGRATION=%s to discard only the legacy retained Alarm topic.\n' \
          "$alarm_topic" \
          "$alarm_legacy_migration_guard" >&2
        return 1
      fi

      "$kafka_topics" \
        --bootstrap-server "$bootstrap_servers" \
        --delete \
        --topic "Alarm"
      wait_for_alarm_topic_deletion
      initialize_compacted_alarm_topic
      return
      ;;
    *)
      printf 'Topic %s has unsupported cleanup.policy=%s.\n' \
        "$alarm_topic" \
        "$cleanup_policy" >&2
      return 1
      ;;
  esac
}

stream_topics=(
  "LiveMeasurement"
  "MeasurementSession"
  "Export"
)

compacted_topics=(
  "Masterdata"
  "Schema"
  "Blobmeta"
)

validate_positive_integer "MEASUREMENT_SESSION_TOPIC_PARTITIONS" "$measurement_session_partitions"
validate_positive_integer "BLOBMETA_TOPIC_PARTITIONS" "$blobmeta_partitions"

for topic in "${stream_topics[@]}"; do
  create_topic "$topic"
  verify_topic_layout "$topic"
  set_cleanup_policy "$topic" "delete"
  verify_cleanup_policy "$topic" "delete"
done

for topic in "${compacted_topics[@]}"; do
  create_topic "$topic"
  verify_topic_layout "$topic"
  set_cleanup_policy "$topic" "compact"
  verify_cleanup_policy "$topic" "compact"
done

reconcile_alarm_topic
reconcile_alarm_evaluation_watermark_topic

echo "WAMA Kafka topics initialized:"
"$kafka_topics" --bootstrap-server "$bootstrap_servers" --list --exclude-internal | sort
