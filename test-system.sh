#!/bin/bash

# Test script for Kafka E-commerce System
echo "Starting Kafka E-commerce System Test"

# Check if Kafka is running
echo "Checking Kafka status..."
docker ps | grep kafka
if [ $? -ne 0 ]; then
  echo "Error: Kafka is not running. Please start Kafka first."
  exit 1
fi

# Check if all required topics exist
echo "\nChecking Kafka topics..."
docker exec kafka-ecommerce_kafka_1 kafka-topics --list --bootstrap-server localhost:9092

# Start each service in the background
echo "\nStarting services..."

# Start Order Service
echo "Starting Order Service on port 8085..."
cd /home/technoidentity/Desktop/kafka-ecommerce/order-service && go run main.go &
ORDER_PID=$!
sleep 2
if ! ps -p $ORDER_PID > /dev/null; then
  echo "Error: Order Service failed to start."
  exit 1
fi

# Start Inventory Service
echo "Starting Inventory Service on port 8080..."
cd /home/technoidentity/Desktop/kafka-ecommerce/inventory-service && go run main.go &
INVENTORY_PID=$!
sleep 2
if ! ps -p $INVENTORY_PID > /dev/null; then
  echo "Error: Inventory Service failed to start."
  kill $ORDER_PID 2>/dev/null
  exit 1
fi

# Start Warehouse Service
echo "Starting Warehouse Service on port 8081..."
cd /home/technoidentity/Desktop/kafka-ecommerce/warehouse-service && go run main.go &
WAREHOUSE_PID=$!
sleep 2
if ! ps -p $WAREHOUSE_PID > /dev/null; then
  echo "Error: Warehouse Service failed to start."
  kill $ORDER_PID $INVENTORY_PID 2>/dev/null
  exit 1
fi

# Start Shipper Service
echo "Starting Shipper Service on port 8082..."
cd /home/technoidentity/Desktop/kafka-ecommerce/shipper-service && go run main.go &
SHIPPER_PID=$!
sleep 2
if ! ps -p $SHIPPER_PID > /dev/null; then
  echo "Error: Shipper Service failed to start."
  kill $ORDER_PID $INVENTORY_PID $WAREHOUSE_PID 2>/dev/null
  exit 1
fi

# Start Notification Service
echo "Starting Notification Service on port 8084..."
cd /home/technoidentity/Desktop/kafka-ecommerce/notification-service && go run main.go &
NOTIFICATION_PID=$!
sleep 2
if ! ps -p $NOTIFICATION_PID > /dev/null; then
  echo "Error: Notification Service failed to start."
  kill $ORDER_PID $INVENTORY_PID $WAREHOUSE_PID $SHIPPER_PID 2>/dev/null
  exit 1
fi

echo "\nAll services started. Sending test order..."

# Send a test order
sleep 5
echo "\nSending test order to Order Service..."
curl -X POST http://localhost:8085/order \
  -H "Content-Type: application/json" \
  -d @/home/technoidentity/Desktop/kafka-ecommerce/test-order.json

echo "\n\nTest order sent. Check the logs of each service to verify the message flow."
echo "Press Ctrl+C to stop all services when done testing."

# Wait for user to press Ctrl+C
trap "kill $ORDER_PID $INVENTORY_PID $WAREHOUSE_PID $SHIPPER_PID $NOTIFICATION_PID; echo '\nStopping all services...'; exit 0" INT
wait
