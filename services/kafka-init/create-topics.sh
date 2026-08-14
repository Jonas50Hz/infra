#!/usr/bin/env bash

set -euo pipefail

bootstrap_servers="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
kafka_topics="/opt/kafka/bin/kafka-topics.sh"
kafka_configs="/opt/kafka/bin/kafka-configs.sh"

create_topic() {
  local topic="$1"

  "$kafka_topics" \
    --bootstrap-server "$bootstrap_servers" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 1 \
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

  description=$(
    "$kafka_topics" \
      --bootstrap-server "$bootstrap_servers" \
      --describe \
      --topic "$topic"
  )

  if [[ ! "$description" =~ PartitionCount:[[:space:]]1[[:space:]] ]] ||
    [[ ! "$description" =~ ReplicationFactor:[[:space:]]1[[:space:]] ]]; then
    printf 'Topic %s must have one partition and replication factor one. Current configuration:\n%s\n' \
      "$topic" \
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
