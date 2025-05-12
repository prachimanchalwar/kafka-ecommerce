package producer

import (
	"encoding/json"
	"fmt"
	"log"

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

var kafkaProducer sarama.SyncProducer

func InitializeKafkaProducer(brokers []string) error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	var err error
	kafkaProducer, err = sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return fmt.Errorf("failed to start Kafka producer: %w", err)
	}
	log.Println("Kafka producer initialized")
	return nil
}

func PublishOrder(orderID, customerID string, items []OrderItem, total float64) error {
	event := OrderEvent{
		OrderID:    orderID,
		CustomerID: customerID,
		Items:      items,
		Total:      total,
	}

	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal order event: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: "OrderReceived",
		Value: sarama.ByteEncoder(data),
	}

	_, _, err = kafkaProducer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message to Kafka: %w", err)
	}
	return nil
}

func CloseKafkaProducer() {
	if kafkaProducer != nil {
		_ = kafkaProducer.Close()
	}

}
