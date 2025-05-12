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

type KPIEvent struct {
	KPIName  string `json:"kpi_name"`
	Metric   string `json:"metric"`
	Value    int    `json:"value"`
	Time     int64  `json:"timestamp"`
}

type NotificationConsumer struct {
	processedOrders map[string]bool
	mutex           sync.Mutex
	brokers         []string
}

func NewNotificationConsumer(brokers []string) *NotificationConsumer {
	return &NotificationConsumer{
		processedOrders: make(map[string]bool),
		brokers:         brokers,
	}
}

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
}

func (c *NotificationConsumer) publishKPI(kpi KPIEvent) {
	log.Printf("KPI: %+v", kpi)
}

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
		c.publishToTopic("DeadLetterQueue", value)
		return
	}

	c.publishToTopic("DeadLetterQueue", errBytes)
}

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
