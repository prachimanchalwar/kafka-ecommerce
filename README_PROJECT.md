# Kafka Ecommerce Microservices Demo

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

### 6. Consume Kafka Topics (Debug)
```bash
docker exec -it <kafka_container_name> /bin/bash
# Then inside the container:
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic OrderReceived --from-beginning
```

## Topics Used
- OrderReceived
- OrderConfirmed
- OrderPickedAndPacked
- Notification
- DeadLetterQueue
- KPIs

## Notes
- Each service logs its work and handles duplicates.
- DeadLetterQueue is used for errors or failed processing.
- Extend with KPI events as needed.

---

This project is simple and runnable for demo/testing. For production, add persistence, config, and improved error handling.
