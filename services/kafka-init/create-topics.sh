#!/usr/bin/env bash

set -euo pipefail

bootstrap_servers="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
kafka_topics="/opt/kafka/bin/kafka-topics.sh"
kafka_configs="/opt/kafka/bin/kafka-configs.sh"
measurement_session_partitions="${MEASUREMENT_SESSION_TOPIC_PARTITIONS:-12}"
blobmeta_partitions="${BLOBMETA_TOPIC_PARTITIONS:-12}"

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

stream_topics=(
  "LiveMeasurement"
  "MeasurementSession"
  "Alarm"
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

echo "WAMA Kafka topics initialized:"
"$kafka_topics" --bootstrap-server "$bootstrap_servers" --list --exclude-internal | sort
