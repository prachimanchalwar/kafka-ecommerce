package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/IBM/sarama"
)

type OrderEvent struct {
	OrderID    string      `json:"order_id"`
	CustomerID string      `json:"customer_id"`
	Items      []OrderItem `json:"items"`
	Total      float64     `json:"total"`
}

type OrderItem struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

var processedOrders = make(map[string]bool)

// KPIEvent struct for KPI publishing
type KPIEvent struct {
	KPIName  string `json:"kpi_name"`
	Metric   string `json:"metric"`
	Value    int    `json:"value"`
	Time     int64  `json:"timestamp"`
}

func publishKPI(kpi KPIEvent) {
	// TODO: Implement KPI publishing to KPIs topic if needed
	log.Printf("KPI: %+v", kpi)
}

func main() {
	log.Println("Starting Shipper Service...")
	go startConsumer()
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	log.Println("Starting HTTP server on port 8082")
	log.Fatal(http.ListenAndServe(":8082", nil))
}

func startConsumer() {
	log.Println("Starting shipper service consumer...")
	
	// Get Kafka brokers from environment variable or use default
	brokers := []string{"localhost:9092"}
	if brokersEnv := os.Getenv("KAFKA_BROKERS"); brokersEnv != "" {
		brokers = []string{brokersEnv}
		log.Printf("Using Kafka brokers from environment: %s", brokersEnv)
	} else {
		log.Printf("Using default Kafka brokers: %s", brokers[0])
	}
	
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	
	// Set client ID for better logging
	config.ClientID = "shipper-service"
	
	log.Printf("Connecting to Kafka brokers: %v", brokers)
	client, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer client.Close()
	
	log.Println("Successfully connected to Kafka")
	
	// Try to consume from the beginning of the topic to see all messages
	log.Println("Attempting to consume from OrderPickedAndPacked topic, partition 0")
	partitionConsumer, err := client.ConsumePartition("OrderPickedAndPacked", 0, sarama.OffsetOldest)
	if err != nil {
		log.Fatalf("Failed to consume partition: %v", err)
	}
	defer partitionConsumer.Close()
	
	log.Println("Successfully created partition consumer, waiting for messages...")

	// Create a channel for errors
	errors := make(chan error, 1)
	
	// Start a goroutine to handle errors
	go func() {
		for err := range partitionConsumer.Errors() {
			log.Printf("Error from partition consumer: %v", err)
			errors <- err
		}
	}()

	for message := range partitionConsumer.Messages() {
		log.Printf("Received message from topic %s, partition %d, offset %d", 
			message.Topic, message.Partition, message.Offset)
		log.Printf("Message value: %s", string(message.Value))
		
		// First try to unmarshal as our expected OrderEvent format
		var event OrderEvent
		if err := json.Unmarshal(message.Value, &event); err != nil {
			log.Printf("Failed to unmarshal as OrderEvent: %v", err)
			
			// Try to unmarshal as a map to handle different formats
			var rawEvent map[string]interface{}
			if err := json.Unmarshal(message.Value, &rawEvent); err != nil {
				log.Printf("Failed to unmarshal as map: %v", err)
				log.Printf("Raw message: %s", string(message.Value))
				publishKPI(KPIEvent{
					KPIName: "shipper_error",
					Metric:  "unmarshal_error",
					Value:   1,
					Time:    time.Now().Unix(),
				})
				publishToDeadLetterQueue(message.Value)
				continue
			}
			
			// Extract order_id from the raw event
			orderID, ok := rawEvent["order_id"].(string)
			if !ok {
				log.Printf("Message doesn't contain a valid order_id")
				publishToDeadLetterQueue(message.Value)
				continue
			}
			
			// Create a minimal event with just the order_id
			event = OrderEvent{
				OrderID: orderID,
			}
			
			// Try to extract customer_id if present
			if customerID, ok := rawEvent["customer_id"].(string); ok {
				event.CustomerID = customerID
			}
			
			// Try to extract total if present
			if total, ok := rawEvent["total"].(float64); ok {
				event.Total = total
			}
			
			log.Printf("Converted message to compatible format: %+v", event)
		}

		log.Printf("Successfully unmarshaled event: %+v", event)
		
		if processedOrders[event.OrderID] {
			log.Printf("Duplicate order picked and packed: %s", event.OrderID)
			continue
		}
		processedOrders[event.OrderID] = true

		log.Printf("Order picked and packed: %+v", event)
		publishKPI(KPIEvent{
			KPIName: "order_picked_and_packed",
			Metric:  "success",
			Value:   1,
			Time:    time.Now().Unix(),
		})
		// Here you would process the order (e.g., shipping logic)
		if event.OrderID == "fail" {
			publishKPI(KPIEvent{
				KPIName: "shipper_error",
				Metric:  "processing_error",
				Value:   1,
				Time:    time.Now().Unix(),
			})
			log.Printf("Failed to process order: %s, sending to DeadLetterQueue", event.OrderID)
			publishToDeadLetterQueue(message.Value)
		} else {
			// Publish notification event
			notification := map[string]interface{}{
				"order_id":    event.OrderID,
				"customer_id": event.CustomerID,
				"message":     "Your order is being shipped!",
			}
			notificationBytes, _ := json.Marshal(notification)
			publishToTopic("Notification", notificationBytes)
			log.Printf("Published shipping notification for OrderID: %s", event.OrderID)
		}
	}
}

func publishToTopic(topic string, value []byte) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	brokers := []string{"localhost:9092"}
	if brokersEnv := os.Getenv("KAFKA_BROKERS"); brokersEnv != "" {
		brokers = []string{brokersEnv}
	}
	
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Printf("Failed to create producer: %v", err)
		return
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("Failed to publish message to %s: %v", topic, err)
		return
	}

	log.Printf("Published message to topic %s, partition %d, offset %d", topic, partition, offset)
}

func publishToDeadLetterQueue(value []byte) {
	publishToTopic("DeadLetterQueue", value)
}
