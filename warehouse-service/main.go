package main

import (
	"encoding/json"
	"log"
	"net/http"
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
	// Log KPI events for monitoring
	log.Printf("KPI: %+v", kpi)
}

func main() {
	go startConsumer()
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	log.Fatal(http.ListenAndServe(":8081", nil))
}

func startConsumer() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	brokers := []string{"localhost:9092"}
	client, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer client.Close()

	partitionConsumer, err := client.ConsumePartition("OrderConfirmed", 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to consume partition: %v", err)
	}
	defer partitionConsumer.Close()

	for message := range partitionConsumer.Messages() {
		var order OrderEvent
		if err := json.Unmarshal(message.Value, &order); err != nil {
			publishKPI(KPIEvent{
				KPIName: "warehouse_error",
				Metric:  "unmarshal_error",
				Value:   1,
				Time:    time.Now().Unix(),
			})
			log.Printf("Failed to unmarshal event: %v", err)
			publishToDeadLetterQueue(message.Value)
			continue
		}
		if processedOrders[order.OrderID] {
			log.Printf("Duplicate order: %s", order.OrderID)
			continue
		}
		processedOrders[order.OrderID] = true
		// Simulate processing
		if order.OrderID == "fail" {
			publishKPI(KPIEvent{
				KPIName: "warehouse_error",
				Metric:  "processing_error",
				Value:   1,
				Time:    time.Now().Unix(),
			})
			log.Printf("Failed to process order: %s, sending to DeadLetterQueue", order.OrderID)
			publishToDeadLetterQueue(message.Value)
		} else {
			publishKPI(KPIEvent{
				KPIName: "order_processed",
				Metric:  "success",
				Value:   1,
				Time:    time.Now().Unix(),
			})
			log.Printf("Order confirmed for OrderID: %s, publishing notification and OrderPickedAndPacked event", order.OrderID)
			// Publish notification event
			notification := map[string]interface{}{
				"order_id": order.OrderID,
				"customer_id": order.CustomerID,
				"message": "Your order is being fulfilled!",
			}
			notificationBytes, _ := json.Marshal(notification)
			publishToTopic("Notification", notificationBytes)
			// Publish OrderPickedAndPacked event with proper format
			// Create a properly formatted OrderEvent to ensure compatibility
			pickedEvent := OrderEvent{
				OrderID:    order.OrderID,
				CustomerID: order.CustomerID,
				Items:      order.Items,
				Total:      order.Total,
			}
			
			// Marshal to ensure proper format
			pickedEventBytes, err := json.Marshal(pickedEvent)
			if err != nil {
				log.Printf("Failed to marshal picked event: %v", err)
				publishToDeadLetterQueue(message.Value)
			} else {
				publishToTopic("OrderPickedAndPacked", pickedEventBytes)
			}
		}
	}
}

func publishToTopic(topic string, value []byte) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Printf("Producer error: %v", err)
		return
	}
	defer producer.Close()
	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(value)}
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		log.Printf("Failed to send message to %s: %v", topic, err)
	} else {
		log.Printf("Published event to %s", topic)
	}
}

func publishToDeadLetterQueue(value []byte) {
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
		publishToTopic("DeadLetterQueue", value) // Fallback to original message if marshaling fails
		return
	}

	publishToTopic("DeadLetterQueue", errBytes)
}
