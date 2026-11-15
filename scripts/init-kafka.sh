#!/usr/bin/env bash

set -euo pipefail

bootstrap_server="${KAFKA_BOOTSTRAP_SERVER:-kafka:29092}"
partitions="${KAFKA_TOPIC_PARTITIONS:-3}"

for topic in transactions.raw transactions.flagged transactions.cleared transactions.dlq; do
  kafka-topics \
    --bootstrap-server "$bootstrap_server" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1
done

echo "Kafka topics initialized on ${bootstrap_server}"
