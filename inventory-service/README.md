# Inventory Service

This service is part of the Kafka E-commerce platform and is responsible for processing order events, checking inventory, and confirming orders.

## Features

- Consumes `OrderReceived` events from Kafka
- Processes orders in an idempotent manner (handles duplicate events)
- Publishes `OrderConfirmed` events for successfully processed orders
- Sends failed events to the `DeadLetterQueue` with error details
- Graceful shutdown handling
- Configurable via environment variables

## Prerequisites

- Go 1.21 or higher
- Apache Kafka running locally or accessible via network
- Required Go dependencies (will be installed automatically)

## Configuration

The service can be configured using the following environment variables:

- `KAFKA_BROKERS`: Comma-separated list of Kafka broker addresses (default: `localhost:9092`)
- `CONSUMER_GROUP_ID`: Consumer group ID (default: `inventory-service`)
- `ORDER_RECEIVED_TOPIC`: Topic to consume order events from (default: `OrderReceived`)
- `ORDER_CONFIRMED_TOPIC`: Topic to publish order confirmations to (default: `OrderConfirmed`)
- `DEAD_LETTER_QUEUE_TOPIC`: Topic for failed events (default: `DeadLetterQueue`)

## Running the Service

1. Start Zookeeper and Kafka:
   ```bash
   # Start Zookeeper
   bin/zookeeper-server-start.sh config/zookeeper.properties
   
   # Start Kafka
   bin/kafka-server-start.sh config/server.properties
   ```

2. Create the required topics:
   ```bash
   bin/kafka-topics.sh --create --topic OrderReceived --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   bin/kafka-topics.sh --create --topic OrderConfirmed --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   bin/kafka-topics.sh --create --topic DeadLetterQueue --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

3. Build and run the service:
   ```bash
   cd inventory-service
   go mod tidy
   go build
   ./inventory-service
   ```

## Testing

1. Send a test order to the `OrderReceived` topic:
   ```bash
   # Start a console producer
   bin/kafka-console-producer.sh --topic OrderReceived --bootstrap-server localhost:9092
   ```
   
   Then paste a JSON order like:
   ```json
   {
     "order_id": "order-123",
     "customer_id": "cust-456",
     "items": [
       {
         "product_id": "prod-789",
         "name": "Test Product",
         "quantity": 2,
         "price": 19.99
       }
     ],
     "total": 39.98
   }
   ```

2. Check the service logs to see the order being processed.

3. Verify the order confirmation was published:
   ```bash
   bin/kafka-console-consumer.sh --topic OrderConfirmed --bootstrap-server localhost:9092 --from-beginning
   ```

4. To test error handling, send an invalid message to the `OrderReceived` topic and check the `DeadLetterQueue`:
   ```bash
   bin/kafka-console-consumer.sh --topic DeadLetterQueue --bootstrap-server localhost:9092 --from-beginning
   ```

## Implementation Details

- **Idempotency**: The service maintains an in-memory map of processed order IDs to handle duplicate events. In a production environment, you might want to use a distributed cache or database for this purpose.

- **Error Handling**: Any errors during message processing are logged and the message is sent to the Dead Letter Queue with error details.

- **Graceful Shutdown**: The service handles SIGINT and SIGTERM signals to perform a clean shutdown, ensuring all in-flight messages are processed before exiting.

## Dependencies

- `github.com/IBM/sarama`: Kafka client library for Go
- `github.com/kelseyhightower/envconfig`: For environment variable configuration
- `github.com/google/uuid`: For generating unique IDs

## License