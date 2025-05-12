package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/Shopify/sarama"
	"github.com/gin-gonic/gin"
)

type OrderEvent struct {
	OrderID    string   `json:"order_id"`
	CustomerID string   `json:"customer_id"`
	Items      []string `json:"items"`
	Total      float64  `json:"total"`
}

func main() {
	producer, err := newKafkaSyncProducer([]string{"localhost:9092"})
	if err != nil {
		log.Fatalf("Failed to start Sarama producer: %v", err)
	}
	defer producer.Close()

	router := gin.Default()

	router.POST("/publish", func(c *gin.Context) {
		var order OrderEvent
		if err := c.BindJSON(&order); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request"})
			return
		}

		eventBytes, err := json.Marshal(order)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to encode event"})
			return
		}

		msg := &sarama.ProducerMessage{
			Topic: "orders",
			Value: sarama.ByteEncoder(eventBytes),
		}

		_, _, err = producer.SendMessage(msg)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to send message"})
			return
		}

		c.JSON(http.StatusOK, gin.H{"status": "Order published successfully"})
	})

	router.Run(":8000")
}

func newKafkaSyncProducer(brokers []string) (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Retry.Max = 5
	config.Producer.RequiredAcks = sarama.WaitForAll

	return sarama.NewSyncProducer(brokers, config)
}
