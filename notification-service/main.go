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
// (You can add a real publisher if you want to emit KPIs)
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
	go startConsumer()
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	log.Fatal(http.ListenAndServe(":8084", nil))
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

	partitionConsumer, err := client.ConsumePartition("Notification", 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to consume partition: %v", err)
	}
	defer partitionConsumer.Close()

	for message := range partitionConsumer.Messages() {
		var event OrderEvent
		if err := json.Unmarshal(message.Value, &event); err != nil {
			publishKPI(KPIEvent{
				KPIName: "notification_error",
				Metric:  "unmarshal_error",
				Value:   1,
				Time:    time.Now().Unix(),
			})
			log.Printf("Failed to unmarshal event: %v", err)
			publishToDeadLetterQueue(message.Value)
			continue
		}

		if processedOrders[event.OrderID] {
			log.Printf("Duplicate notification: %s", event.OrderID)
			continue
		}
		processedOrders[event.OrderID] = true

		log.Printf("Notification received and processed: %+v", event)
		publishKPI(KPIEvent{
			KPIName: "notification_processed",
			Metric:  "success",
			Value:   1,
			Time:    time.Now().Unix(),
		})
		// Here you would process the notification (e.g., send email/SMS)
	}
}

func publishToDeadLetterQueue(value []byte) {
	errEvent := struct {
		OriginalEvent json.RawMessage `json:"original_event"`
		Error         string          `json:"error"`
		Timestamp     time.Time       `json:"timestamp"`
	}{
		OriginalEvent: value,
		Error:         "Failed to process notification",
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
