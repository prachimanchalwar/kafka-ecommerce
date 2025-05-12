#!/bin/bash
# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 10

KAFKA_BROKER=localhost:9092
TOPICS=(OrderReceived OrderConfirmed OrderPickedAndPacked Notification DeadLetterQueue KPIs)

for TOPIC in "${TOPICS[@]}"; do
  bin/kafka-topics.sh --create --if-not-exists --topic "$TOPIC" --bootstrap-server $KAFKA_BROKER --partitions 1 --replication-factor 1
  echo "Created topic $TOPIC"
done
