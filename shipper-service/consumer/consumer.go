package consumer

import (
	"encoding/json"
	"log"
	"os"
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

type ShipperConsumer struct {
	processedOrders map[string]bool
	mutex           sync.Mutex
	brokers         []string
}

func NewShipperConsumer(brokers []string) *ShipperConsumer {
	return &ShipperConsumer{
		processedOrders: make(map[string]bool),
		brokers:         brokers,
	}
}

func (c *ShipperConsumer) Start() error {
	log.Println("Starting shipper service consumer...")
	
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	
	config.ClientID = "shipper-service"
	
	log.Printf("Connecting to Kafka brokers: %v", c.brokers)
	client, err := sarama.NewConsumer(c.brokers, config)
	if err != nil {
		return err
	}
	defer client.Close()
	
	log.Println("Successfully connected to Kafka")
	
	log.Println("Attempting to consume from OrderPickedAndPacked topic, partition 0")
	partitionConsumer, err := client.ConsumePartition("OrderPickedAndPacked", 0, sarama.OffsetOldest)
	if err != nil {
		return err
	}
	defer partitionConsumer.Close()
	
	log.Println("Successfully created partition consumer, waiting for messages...")

	errors := make(chan error, 1)
	
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

func (c *ShipperConsumer) processMessage(message *sarama.ConsumerMessage) {
	log.Printf("Received message from topic %s, partition %d, offset %d", 
		message.Topic, message.Partition, message.Offset)
	log.Printf("Message value: %s", string(message.Value))
	
	var event OrderEvent
	if err := json.Unmarshal(message.Value, &event); err != nil {
		log.Printf("Failed to unmarshal as OrderEvent: %v", err)
		
		var rawEvent map[string]interface{}
		if err := json.Unmarshal(message.Value, &rawEvent); err != nil {
			log.Printf("Failed to unmarshal as map: %v", err)
			log.Printf("Raw message: %s", string(message.Value))
			c.publishKPI(KPIEvent{
				KPIName: "shipper_error",
				Metric:  "unmarshal_error",
				Value:   1,
				Time:    time.Now().Unix(),
			})
			c.publishToDeadLetterQueue(message.Value)
			return
		}
		
		orderID, ok := rawEvent["order_id"].(string)
		if !ok {
			log.Printf("Message doesn't contain a valid order_id")
			c.publishToDeadLetterQueue(message.Value)
			return
		}
		
		event = OrderEvent{
			OrderID: orderID,
		}
		
		if customerID, ok := rawEvent["customer_id"].(string); ok {
			event.CustomerID = customerID
		}
		
		if total, ok := rawEvent["total"].(float64); ok {
			event.Total = total
		}
		
		log.Printf("Converted message to compatible format: %+v", event)
	}

	log.Printf("Successfully unmarshaled event: %+v", event)
	
	c.mutex.Lock()
	if c.processedOrders[event.OrderID] {
		log.Printf("Duplicate order picked and packed: %s", event.OrderID)
		c.mutex.Unlock()
		return
	}
	c.processedOrders[event.OrderID] = true
	c.mutex.Unlock()

	log.Printf("Order picked and packed: %+v", event)
	c.publishKPI(KPIEvent{
		KPIName: "order_picked_and_packed",
		Metric:  "success",
		Value:   1,
		Time:    time.Now().Unix(),
	})
	
	if event.OrderID == "fail" {
		c.publishKPI(KPIEvent{
			KPIName: "shipper_error",
			Metric:  "processing_error",
			Value:   1,
			Time:    time.Now().Unix(),
		})
		log.Printf("Failed to process order: %s, sending to DeadLetterQueue", event.OrderID)
		c.publishToDeadLetterQueue(message.Value)
	} else {
		notification := map[string]interface{}{
			"order_id":    event.OrderID,
			"customer_id": event.CustomerID,
			"message":     "Your order is being shipped!",
		}
		notificationBytes, _ := json.Marshal(notification)
		c.publishToTopic("Notification", notificationBytes)
		log.Printf("Published shipping notification for OrderID: %s", event.OrderID)
	}
}

func (c *ShipperConsumer) publishKPI(kpi KPIEvent) {
	log.Printf("KPI: %+v", kpi)
}

func (c *ShipperConsumer) publishToDeadLetterQueue(value []byte) {
	errEvent := struct {
		OriginalEvent json.RawMessage `json:"original_event"`
		Error         string          `json:"error"`
		Timestamp     time.Time       `json:"timestamp"`
	}{
		OriginalEvent: value,
		Error:         "Failed to process shipping event",
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

func (c *ShipperConsumer) publishToTopic(topic string, value []byte) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	brokers := c.brokers
	if brokersEnv := os.Getenv("KAFKA_BROKERS"); brokersEnv != "" {
		brokers = []string{brokersEnv}
	}
	
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Printf("Failed to create producer: %v", err)
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
