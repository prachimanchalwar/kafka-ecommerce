package main

import (
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/prachi/kafka-ecommerce/order-service/producer"
)

func main() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "localhost:9092"
	}

	if err := producer.InitializeKafkaProducer([]string{brokers}); err != nil {
		log.Fatalf("Failed to initialize Kafka producer: %v", err)
	}
	defer producer.CloseKafkaProducer()

	r := gin.Default()

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	r.POST("/order", func(c *gin.Context) {
		var input struct {
			OrderID    string             `json:"order_id"`
			CustomerID string             `json:"customer_id"`
			Items      []producer.OrderItem `json:"items"`
			Total      float64            `json:"total"`
		}

		if err := c.ShouldBindJSON(&input); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		err := producer.PublishOrder(input.OrderID, input.CustomerID, input.Items, input.Total)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to publish order"})
			return
		}

		c.JSON(http.StatusOK, gin.H{"message": "Order published successfully"})
	})

	if err := r.Run(":8085"); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}
}