package consumer

import (
	"encoding/json"
	"log"
	"sync"
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

// KPIEvent struct for KPI publishing
type KPIEvent struct {
	KPIName  string `json:"kpi_name"`
	Metric   string `json:"metric"`
	Value    int    `json:"value"`
	Time     int64  `json:"timestamp"`
}

// WarehouseConsumer handles consuming warehouse events
type WarehouseConsumer struct {
	processedOrders map[string]bool
	mutex           sync.Mutex
	brokers         []string
}

// NewWarehouseConsumer creates a new warehouse consumer
func NewWarehouseConsumer(brokers []string) *WarehouseConsumer {
	return &WarehouseConsumer{
		processedOrders: make(map[string]bool),
		brokers:         brokers,
	}
}

// Start begins consuming from the OrderConfirmed topic
func (c *WarehouseConsumer) Start() error {
	log.Println("Starting warehouse service consumer...")
	
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	
	// Set client ID for better logging
	config.ClientID = "warehouse-service"
	
	log.Printf("Connecting to Kafka brokers: %v", c.brokers)
	client, err := sarama.NewConsumer(c.brokers, config)
	if err != nil {
		return err
	}
	defer client.Close()
	
	log.Println("Successfully connected to Kafka")
	
	// Consume from the OrderConfirmed topic
	log.Println("Attempting to consume from OrderConfirmed topic, partition 0")
	partitionConsumer, err := client.ConsumePartition("OrderConfirmed", 0, sarama.OffsetNewest)
	if err != nil {
		return err
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
		c.processMessage(message)
	}

	return nil
}

// processMessage handles an individual Kafka message
func (c *WarehouseConsumer) processMessage(message *sarama.ConsumerMessage) {
	log.Printf("Received message from topic %s, partition %d, offset %d", 
		message.Topic, message.Partition, message.Offset)
	
	var order OrderEvent
	if err := json.Unmarshal(message.Value, &order); err != nil {
		c.publishKPI(KPIEvent{
			KPIName: "warehouse_error",
			Metric:  "unmarshal_error",
			Value:   1,
			Time:    time.Now().Unix(),
		})
		log.Printf("Failed to unmarshal event: %v", err)
		c.publishToDeadLetterQueue(message.Value)
		return
	}
	
	// Check for duplicate processing (idempotency)
	c.mutex.Lock()
	if c.processedOrders[order.OrderID] {
		log.Printf("Duplicate order: %s", order.OrderID)
		c.mutex.Unlock()
		return
	}
	c.processedOrders[order.OrderID] = true
	c.mutex.Unlock()

	// Simulate processing
	if order.OrderID == "fail" {
		c.publishKPI(KPIEvent{
			KPIName: "warehouse_error",
			Metric:  "processing_error",
			Value:   1,
			Time:    time.Now().Unix(),
		})
		log.Printf("Failed to process order: %s, sending to DeadLetterQueue", order.OrderID)
		c.publishToDeadLetterQueue(message.Value)
	} else {
		c.publishKPI(KPIEvent{
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
		c.publishToTopic("Notification", notificationBytes)
		
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
			c.publishToDeadLetterQueue(message.Value)
		} else {
			c.publishToTopic("OrderPickedAndPacked", pickedEventBytes)
		}
	}
}

// publishKPI publishes KPI metrics to the KPIs topic
func (c *WarehouseConsumer) publishKPI(kpi KPIEvent) {
	// Log KPI events for monitoring
	log.Printf("KPI: %+v", kpi)
	
	// Optionally publish to a KPIs topic
	kpiBytes, _ := json.Marshal(kpi)
	c.publishToTopic("KPIs", kpiBytes)
}

// publishToDeadLetterQueue sends failed messages to the DLQ
func (c *WarehouseConsumer) publishToDeadLetterQueue(value []byte) {
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
		c.publishToTopic("DeadLetterQueue", value) // Fallback to original message if marshaling fails
		return
	}

	c.publishToTopic("DeadLetterQueue", errBytes)
}

// publishToTopic publishes a message to the specified Kafka topic
func (c *WarehouseConsumer) publishToTopic(topic string, value []byte) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	
	producer, err := sarama.NewSyncProducer(c.brokers, config)
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
