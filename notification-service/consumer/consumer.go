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

// NotificationConsumer handles consuming notification events
type NotificationConsumer struct {
	processedOrders map[string]bool
	mutex           sync.Mutex
	brokers         []string
}

// NewNotificationConsumer creates a new notification consumer
func NewNotificationConsumer(brokers []string) *NotificationConsumer {
	return &NotificationConsumer{
		processedOrders: make(map[string]bool),
		brokers:         brokers,
	}
}

// Start begins consuming from the Notification topic
func (c *NotificationConsumer) Start() error {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	
	client, err := sarama.NewConsumer(c.brokers, config)
	if err != nil {
		return err
	}
	defer client.Close()

	partitionConsumer, err := client.ConsumePartition("Notification", 0, sarama.OffsetNewest)
	if err != nil {
		return err
	}
	defer partitionConsumer.Close()

	log.Println("Notification consumer started, waiting for messages...")
	
	for message := range partitionConsumer.Messages() {
		c.processMessage(message)
	}

	return nil
}

// processMessage handles an individual Kafka message
func (c *NotificationConsumer) processMessage(message *sarama.ConsumerMessage) {
	var event OrderEvent
	if err := json.Unmarshal(message.Value, &event); err != nil {
		c.publishKPI(KPIEvent{
			KPIName: "notification_error",
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
	if c.processedOrders[event.OrderID] {
		log.Printf("Duplicate notification: %s", event.OrderID)
		c.mutex.Unlock()
		return
	}
	c.processedOrders[event.OrderID] = true
	c.mutex.Unlock()

	log.Printf("Notification received and processed: %+v", event)
	c.publishKPI(KPIEvent{
		KPIName: "notification_processed",
		Metric:  "success",
		Value:   1,
		Time:    time.Now().Unix(),
	})
	// Here you would process the notification (e.g., send email/SMS)
}

// publishKPI publishes KPI metrics to the KPIs topic
func (c *NotificationConsumer) publishKPI(kpi KPIEvent) {
	// TODO: Implement KPI publishing to KPIs topic if needed
	log.Printf("KPI: %+v", kpi)
}

// publishToDeadLetterQueue sends failed messages to the DLQ
func (c *NotificationConsumer) publishToDeadLetterQueue(value []byte) {
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
		c.publishToTopic("DeadLetterQueue", value) // Fallback to original message if marshaling fails
		return
	}

	c.publishToTopic("DeadLetterQueue", errBytes)
}

// publishToTopic publishes a message to the specified Kafka topic
func (c *NotificationConsumer) publishToTopic(topic string, value []byte) {
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
