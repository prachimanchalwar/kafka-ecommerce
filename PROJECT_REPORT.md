# Kafka E-commerce Microservices Project Report

## Project Overview

This project implements a distributed e-commerce system using event-driven microservices architecture with Apache Kafka as the messaging backbone. The system demonstrates how different services in an e-commerce platform can communicate asynchronously through events, ensuring loose coupling and high scalability.

The codebase follows a clean, modular structure with consistent patterns across all services, making it maintainable and extensible.

## Architecture

The system consists of five microservices that communicate through Kafka topics:

1. **Order Service**: Entry point for customer orders via REST API
2. **Inventory Service**: Validates inventory availability
3. **Warehouse Service**: Handles order fulfillment
4. **Shipper Service**: Manages shipping logistics
5. **Notification Service**: Sends customer notifications

### Communication Flow

```
Order Service → Inventory Service → Warehouse Service → Shipper Service → Notification Service
```

### Kafka Topics

- **OrderReceived**: Orders from Order Service to Inventory Service
- **OrderConfirmed**: Confirmed orders from Inventory Service to Warehouse Service
- **OrderPickedAndPacked**: Fulfilled orders from Warehouse Service to Shipper Service
- **Notification**: Notifications from various services to Notification Service
- **DeadLetterQueue**: Failed messages for error handling
- **KPIs**: Performance metrics

## Implementation Details

### Technologies Used

- **Go**: All microservices are implemented in Go
- **Sarama**: Kafka client library for Go
- **Docker & Docker Compose**: For containerization and orchestration

### Project Structure

Each microservice follows a consistent structure:

```
service-name/
  ├── config/        # Configuration management
  │   └── config.go  # Service configuration
  ├── consumer/      # Kafka consumer implementation
  │   └── consumer.go # Consumer logic
  ├── go.mod         # Go module definition
  ├── go.sum         # Go module checksums
  └── main.go        # Service entry point
```

Shared components:

```
scripts/           # Utility scripts
  ├── init-kafka.sh  # Initialize Kafka topics
  └── test-system.sh # Test the entire system
schemas/           # JSON schemas for events
docker-compose.yml # Docker configuration
```

### Key Features

#### 1. Idempotent Event Processing

Each service implements idempotent processing to prevent duplicate processing of messages. This is achieved by maintaining a map of processed order IDs:

```go
type WarehouseConsumer struct {
	processedOrders map[string]bool
	mutex           sync.Mutex
	brokers         []string
}

// Check for duplicate processing (idempotency)
c.mutex.Lock()
if c.processedOrders[order.OrderID] {
	log.Printf("Duplicate order: %s", order.OrderID)
	c.mutex.Unlock()
	return
}
c.processedOrders[order.OrderID] = true
c.mutex.Unlock()
```

#### 2. Error Handling with DeadLetterQueue

When a service fails to process a message, it publishes the original message along with error details to a DeadLetterQueue topic:

```go
func (c *WarehouseConsumer) publishToDeadLetterQueue(value []byte) {
	errEvent := struct {
		OriginalEvent json.RawMessage `json:"original_event"`
		Error         string          `json:"error"`
		Timestamp     time.Time       `json:"timestamp"`
	}{
		OriginalEvent: value,
		Error:         "Failed to process order",
		Timestamp:     time.Now().UTC(),
	}

	errBytes, err := json.Marshal(errEvent)
	if err != nil {
		log.Printf("Failed to marshal error event: %v", err)
		c.publishToTopic("DeadLetterQueue", value) // Fallback to original message if marshaling fails
		return
	}

	c.publishToTopic("DeadLetterQueue", errBytes)
}
```

#### 3. KPI Metrics

The system tracks various KPIs to evaluate performance:

```go
type KPIEvent struct {
	KPIName  string `json:"kpi_name"`
	Metric   string `json:"metric"`
	Value    int    `json:"value"`
	Time     int64  `json:"timestamp"`
}

func (c *WarehouseConsumer) publishKPI(kpi KPIEvent) {
	// Log KPI events for monitoring
	log.Printf("KPI: %+v", kpi)
	
	// Optionally publish to a KPIs topic
	kpiBytes, _ := json.Marshal(kpi)
	c.publishToTopic("KPIs", kpiBytes)
}
```

Examples of KPIs tracked:
- Order processing latency
- Errors per minute
- Successful order processing counts

### Service-Specific Implementations

#### Inventory Service

The Inventory Service consumes from the OrderReceived topic and validates inventory availability. It implements:

- Idempotent processing to prevent duplicate order processing
- Error handling with DeadLetterQueue
- KPI tracking for performance monitoring
- Publishing confirmed orders to the OrderConfirmed topic

#### Warehouse Service

The Warehouse Service consumes from the OrderConfirmed topic and handles order fulfillment:

- Processes confirmed orders
- Publishes notifications to the Notification topic
- Publishes picked and packed orders to the OrderPickedAndPacked topic
- Implements error handling and KPI tracking

#### Shipper Service

The Shipper Service consumes from the OrderPickedAndPacked topic and manages shipping logistics:

- Processes shipping events
- Publishes shipping notifications to the Notification topic
- Implements error handling and KPI tracking

#### Notification Service

The Notification Service consumes from the Notification topic and processes notification events:

- Handles notifications from all services
- Implements idempotent processing
- Tracks KPIs for notification delivery

## Testing

The project includes scripts for testing the system:

```bash
#!/bin/bash
# Test order payload
ORDER_PAYLOAD='{
  "order_id": "order-'$(date +%s)'",
  "customer_id": "cust-456",
  "items": [
    {
      "product_id": "prod-789",
      "name": "Test Product",
      "quantity": 2,
      "price": 19.99
    }
  ],
  "total": 39.98,
  "timestamp": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'"
}'

# Publishing to OrderReceived topic
echo "$ORDER_PAYLOAD" | kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic OrderReceived
```

## Deployment

The system is deployed using Docker and Docker Compose:

```yaml
# docker-compose.yml
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.0.1
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"
    volumes:
      - zookeeper_data:/var/lib/zookeeper

  kafka:
    image: confluentinc/cp-kafka:7.0.1
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: 'true'
      KAFKA_CREATE_TOPICS: "OrderReceived:1:1,OrderConfirmed:1:1,OrderPickedAndPacked:1:1,Notification:1:1,DeadLetterQueue:1:1,KPIs:1:1"
    volumes:
      - kafka_data:/var/lib/kafka
```

## Future Enhancements

1. **Persistence**: Add database integration for storing order and inventory data
2. **Authentication**: Implement secure API endpoints
3. **Monitoring**: Add comprehensive monitoring with Prometheus and Grafana
4. **Scaling**: Implement horizontal scaling for services
5. **Resilience**: Add circuit breakers and retry mechanisms
6. **Shared Library**: Create a common library for shared code across services
7. **API Gateway**: Implement an API gateway for external access
8. **Service Discovery**: Add service discovery for dynamic scaling

## Conclusion

This Kafka e-commerce microservices project demonstrates the implementation of an event-driven architecture for e-commerce systems. The use of Kafka as a message broker enables loose coupling between services, making the system more maintainable and scalable.

The implementation of idempotent processing, error handling, and KPI tracking ensures the system is robust and observable. The consistent structure across all services makes the codebase easy to understand and extend.
