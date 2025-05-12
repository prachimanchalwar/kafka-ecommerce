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
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run check-topic.go [topic_name]")
		return
	}

	topicName := os.Args[1]
	consumeMessages(topicName)
}

func consumeMessages(topicName string) {
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

	// Check if our topic exists
	topics, err := client.Topics()
	if err != nil {
		log.Fatalf("Failed to get topics: %v", err)
	}
	log.Printf("Available topics: %v", topics)

	topicExists := false
	for _, topic := range topics {
		if topic == topicName {
			topicExists = true
			break
		}
	}

	if !topicExists {
		log.Fatalf("Topic '%s' does not exist", topicName)
	}

	// Get partitions for the topic
	partitions, err := client.Partitions(topicName)
	if err != nil {
		log.Fatalf("Failed to get partitions: %v", err)
	}
	log.Printf("Partitions for %s: %v", topicName, partitions)

	// Try to consume from the beginning of the topic to see all messages
	log.Printf("Attempting to consume from %s topic, partition 0", topicName)
	partitionConsumer, err := client.ConsumePartition(topicName, 0, sarama.OffsetOldest)
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
