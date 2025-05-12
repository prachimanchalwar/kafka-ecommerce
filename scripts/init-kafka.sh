#!/bin/bash
# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
sleep 30

KAFKA_BROKER=localhost:9092
TOPICS=(OrderReceived OrderConfirmed OrderPickedAndPacked Notification DeadLetterQueue KPIs)

# Get the Kafka container name instead of ID
KAFKA_CONTAINER=$(docker ps | grep kafka | grep -v zookeeper | awk '{print $NF}')

if [ -z "$KAFKA_CONTAINER" ]; then
  echo "Error: Kafka container not found"
  exit 1
fi

echo "Using Kafka container: $KAFKA_CONTAINER"

for TOPIC in "${TOPICS[@]}"; do
  echo "Creating topic: $TOPIC"
  docker exec -it $KAFKA_CONTAINER kafka-topics --create --if-not-exists --topic "$TOPIC" --bootstrap-server $KAFKA_BROKER --partitions 1 --replication-factor 1
  echo "Created topic $TOPIC"
done
