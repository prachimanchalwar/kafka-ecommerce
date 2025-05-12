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

echo "Sending test order:"
echo "$ORDER_PAYLOAD" | jq .

echo -e "\nPublishing to OrderReceived topic..."
echo "$ORDER_PAYLOAD" | kafka-console-producer \
  --bootstrap-server localhost:9092 \
  --topic OrderReceived

echo -e "\nListening for OrderConfirmed events (Ctrl+C to exit):"
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic OrderConfirmed \
  --from-beginning \
  --timeout-ms 5000

echo -e "\nChecking DeadLetterQueue (if any):"
kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic DeadLetterQueue \
  --from-beginning \
  --timeout-ms 2000
