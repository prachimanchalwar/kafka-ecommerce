# Kafka E-commerce Microservices

A demonstration of microservices architecture using Kafka for event-driven communication between services in an e-commerce scenario.

## Architecture

This project consists of five microservices that communicate through Kafka topics:

1. **Order Service**: Entry point for customer orders via REST API
2. **Inventory Service**: Validates inventory availability
3. **Warehouse Service**: Handles order fulfillment
4. **Shipper Service**: Manages shipping logistics
5. **Notification Service**: Sends customer notifications

## Kafka Topics

- **OrderReceived**: Orders from Order Service to Inventory Service
- **OrderConfirmed**: Confirmed orders from Inventory Service to Warehouse Service
- **OrderPickedAndPacked**: Fulfilled orders from Warehouse Service to Shipper Service
- **Notification**: Notifications from various services to Notification Service
- **DeadLetterQueue**: Failed messages for error handling
- **KPIs**: Performance metrics

## Technologies

- **Go**: All microservices are implemented in Go
- **Sarama**: Kafka client library for Go
- **Gin**: HTTP web framework for Go
- **Docker & Docker Compose**: For containerization and orchestration

## Prerequisites

- Docker & Docker Compose
- Go 1.17+

## Setup & Run

### 1. Start Kafka and Zookeeper
```bash
docker-compose up -d
```

### 2. Initialize Kafka Topics
```bash
# In a new terminal
./init-kafka.sh
```

### 3. Build & Run Services (in separate terminals)
```bash
# Inventory Service
cd inventory-service && go run main.go

# Warehouse Service
cd ../warehouse-service && go run main.go

# Shipper Service
cd ../shipper-service && go run main.go

# Notification Service
cd ../notification-service && go run main.go
```

### 4. Health Check
```bash
curl localhost:8080/health # Inventory
curl localhost:8081/health # Warehouse
curl localhost:8082/health # Shipper
curl localhost:8083/health # Notification
```

### 5. Send a Test Order
```bash
curl -X POST localhost:8080/order -H "Content-Type: application/json" -d '{"order_id":"123","customer_id":"John","items":[],"total":100}'
```

## Features

- **Event-Driven Architecture**: Services communicate asynchronously via Kafka
- **Idempotent Processing**: Prevents duplicate processing of messages
- **Error Handling**: Failed messages are sent to a Dead Letter Queue
- **KPI Metrics**: Performance tracking for system evaluation

## Project Structure

```
├── inventory-service/   # Validates inventory availability
├── notification-service/ # Sends customer notifications
├── shipper-service/     # Manages shipping logistics
├── warehouse-service/   # Handles order fulfillment
├── docker-compose.yml   # Docker configuration
└── init-kafka.sh        # Script to initialize Kafka topics
```

## License

MIT
