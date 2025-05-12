package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
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
	// Test both producing and consuming from the OrderPickedAndPacked topic
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run test-shipper.go [produce|consume]")
		return
	}

	mode := os.Args[1]

	switch mode {
	case "produce":
		produceMessage()
	case "consume":
		consumeMessages()
	default:
		fmt.Println("Invalid mode. Use 'produce' or 'consume'")
	}
}

func produceMessage() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	brokers := []string{"localhost:9092"}

	log.Printf("Connecting to Kafka brokers: %v", brokers)
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Create a test order event
	orderEvent := OrderEvent{
		OrderID:    fmt.Sprintf("test-order-%d", time.Now().Unix()),
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
}

func consumeMessages() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	brokers := []string{"localhost:9092"}

	log.Printf("Connecting to Kafka brokers: %v", brokers)
	client, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer client.Close()

	log.Println("Successfully connected to Kafka")

	// List all topics
	topics, err := client.Topics()
	if err != nil {
		log.Fatalf("Failed to get topics: %v", err)
	}
	log.Printf("Available topics: %v", topics)

	// Check if our topic exists
	topicExists := false
	for _, topic := range topics {
		if topic == "OrderPickedAndPacked" {
			topicExists = true
			break
		}
	}

	if !topicExists {
		log.Fatalf("Topic 'OrderPickedAndPacked' does not exist")
	}

	// Get partitions for the topic
	partitions, err := client.Partitions("OrderPickedAndPacked")
	if err != nil {
		log.Fatalf("Failed to get partitions: %v", err)
	}
	log.Printf("Partitions for OrderPickedAndPacked: %v", partitions)

	// Try to consume from the beginning of the topic to see all messages
	log.Println("Attempting to consume from OrderPickedAndPacked topic, partition 0")
	partitionConsumer, err := client.ConsumePartition("OrderPickedAndPacked", 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to consume partition: %v", err)
	}
	defer partitionConsumer.Close()

	log.Println("Successfully created partition consumer, waiting for messages...")

	// Set a timeout for consuming messages
	timeout := time.After(30 * time.Second)
	messageCount := 0

	// Create a channel for errors
	errors := make(chan error, 1)

	// Start a goroutine to handle errors
	go func() {
		for err := range partitionConsumer.Errors() {
			log.Printf("Error from partition consumer: %v", err)
			errors <- err
		}
	}()

	// Process messages
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			messageCount++
			log.Printf("Received message %d from topic %s, partition %d, offset %d", 
				messageCount, msg.Topic, msg.Partition, msg.Offset)
			log.Printf("Message value: %s", string(msg.Value))

			var event OrderEvent
			if err := json.Unmarshal(msg.Value, &event); err != nil {
				log.Printf("Failed to unmarshal event: %v", err)
				log.Printf("Raw message: %s", string(msg.Value))
				continue
			}

			log.Printf("Successfully unmarshaled event: %+v", event)

		case <-timeout:
			log.Printf("Timeout reached after consuming %d messages", messageCount)
			return
		case err := <-errors:
			log.Printf("Error consuming from Kafka: %v", err)
			return
		}
	}
}
