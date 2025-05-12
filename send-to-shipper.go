package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type OrderItem struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

type OrderEvent struct {
	OrderID    string      `json:"order_id"`
	CustomerID string      `json:"customer_id"`
	Items      []OrderItem `json:"items"`
	Total      float64     `json:"total"`
}

func main() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	brokers := []string{"localhost:9092"}
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Create a test order event
	orderEvent := OrderEvent{
		OrderID:    "test-order-123",
		CustomerID: "customer-456",
		Items: []OrderItem{
			{
				ProductID: "product-789",
				Quantity:  2,
				Price:     29.99,
			},
		},
		Total: 59.98,
	}

	// Marshal the order event to JSON
	orderJSON, err := json.Marshal(orderEvent)
	if err != nil {
		log.Fatalf("Failed to marshal order event: %v", err)
	}

	// Create a Kafka message
	message := &sarama.ProducerMessage{
		Topic: "OrderPickedAndPacked",
		Value: sarama.StringEncoder(orderJSON),
	}

	// Send the message
	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		log.Fatalf("Failed to send message: %v", err)
	}

	log.Printf("Message sent to partition %d at offset %d", partition, offset)
	log.Printf("Order event: %+v", orderEvent)
	log.Printf("JSON: %s", orderJSON)

	// Wait a moment to ensure the message is processed
	time.Sleep(time.Second * 2)
}
