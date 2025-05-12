package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/prachi/kafka-ecommerce/inventory-service/config"
	"github.com/prachi/kafka-ecommerce/inventory-service/consumer"
)

type KPIEvent struct {
	KPIName   string      `json:"kpi_name"`
	Metric    string      `json:"metric"`
	Value     interface{} `json:"value"`
	OrderID   string      `json:"order_id,omitempty"`
	Timestamp int64       `json:"timestamp"`
}

func publishKPI(event KPIEvent, topic string) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Printf("[KPI] Producer error: %v", err)
		return
	}
	defer producer.Close()
	msgBytes, _ := json.Marshal(event)
	msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(msgBytes)}
	_, _, err = producer.SendMessage(msg)
	if err != nil {
		log.Printf("[KPI] Failed to send KPI event to %s: %v", topic, err)
	} else {
		log.Printf("[KPI] Published KPI event to %s: %s", topic, string(msgBytes))
	}
}

func main() {
	go func() {
		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		})
		http.HandleFunc("/order", func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			var order map[string]interface{}
			log.Println("[POST /order] Received request")
			start := time.Now()
			if err := json.NewDecoder(r.Body).Decode(&order); err != nil {
				log.Println("[POST /order] Invalid JSON")
				publishKPI(KPIEvent{
					KPIName:   "inventory_consumer_errors",
					Metric:    "errors_per_minute",
					Value:     1,
					Timestamp: time.Now().Unix(),
				}, "InventoryKPI")
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("Invalid JSON"))
				return
			}
			msg, _ := json.Marshal(order)
			log.Println("[POST /order] Creating Kafka producer...")
			config := sarama.NewConfig()
			config.Producer.Return.Successes = true
			producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
			if err != nil {
				log.Printf("[POST /order] Kafka producer error: %v", err)
				publishKPI(KPIEvent{
					KPIName:   "inventory_consumer_errors",
					Metric:    "errors_per_minute",
					Value:     1,
					Timestamp: time.Now().Unix(),
				}, "InventoryKPI")
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("Kafka producer error"))
				return
			}
			defer producer.Close()
			log.Println("[POST /order] Sending message to OrderReceived topic...")
			_, _, err = producer.SendMessage(&sarama.ProducerMessage{
				Topic: "OrderReceived",
				Value: sarama.ByteEncoder(msg),
			})
			if err != nil {
				log.Printf("[POST /order] Kafka send error: %v", err)
				publishKPI(KPIEvent{
					KPIName:   "inventory_consumer_errors",
					Metric:    "errors_per_minute",
					Value:     1,
					Timestamp: time.Now().Unix(),
				}, "InventoryKPI")
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("Kafka send error"))
				return
			}
			log.Println("[POST /order] Order sent to Kafka successfully")
			// Publish latency KPI
			latencyMs := time.Since(start).Milliseconds()
			publishKPI(KPIEvent{
				KPIName:   "order_processing_latency",
				Metric:    "latency_ms",
				Value:     latencyMs,
				OrderID:   order["order_id"].(string),
				Timestamp: time.Now().Unix(),
			}, "OrderLatencyKPI")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Order sent to Kafka"))
		})
		log.Println("HTTP server listening on :8080 (/health, /order)")
		http.ListenAndServe(":8080", nil)
	}()

	cfg, err := config.New()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	invConsumer := consumer.NewInventoryConsumer()

	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	errChan := make(chan error, 1)
	go func() {
		if err := invConsumer.Start(cfg); err != nil {
			errChan <- err
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errChan:
		log.Fatalf("Consumer error: %v", err)
	case sig := <-sigChan:
		log.Printf("Received signal %v, shutting down...", sig)
		cancel()
	}

	log.Println("Inventory service stopped")
}
