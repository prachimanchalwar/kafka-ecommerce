package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/prachi/kafka-ecommerce/inventory-service/config"
)

type KPIEvent struct {
	KPIName string `json:"kpi_name"`
	Metric  string `json:"metric"`
	Value   int    `json:"value"`
	Time    int64  `json:"timestamp"`
}

func (c *InventoryConsumer) publishKPI(kpi KPIEvent) {
	if c.producer == nil {
		return
	}
	bytes, _ := json.Marshal(kpi)
	msg := &sarama.ProducerMessage{Topic: "KPIs", Value: sarama.ByteEncoder(bytes)}
	_, _, _ = c.producer.SendMessage(msg)
}

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

type InventoryConsumer struct {
	processedOrders map[string]bool
	mutex           sync.Mutex
	producer        sarama.SyncProducer
}

func NewInventoryConsumer() *InventoryConsumer {
	return &InventoryConsumer{
		processedOrders: make(map[string]bool),
	}
}

func (c *InventoryConsumer) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (c *InventoryConsumer) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (c *InventoryConsumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("Message claimed: topic=%s, partition=%d, offset=%d", msg.Topic, msg.Partition, msg.Offset)

		var event OrderEvent
		err := json.Unmarshal(msg.Value, &event)
		if err != nil {
			c.publishKPI(KPIEvent{
				KPIName: "order_processing_error",
				Metric:  "unmarshal_error",
				Value:   1,
				Time:    time.Now().Unix(),
			})
			errMsg := fmt.Sprintf("failed to unmarshal event: %v", err)
			log.Printf("Error: %s", errMsg)
			c.publishToDeadLetter(msg.Value, fmt.Errorf(errMsg))
			session.MarkMessage(msg, "")
			continue
		}

		c.mutex.Lock()
		if c.processedOrders[event.OrderID] {
			log.Printf("Duplicate order detected and ignored: %s", event.OrderID)
			c.mutex.Unlock()
			session.MarkMessage(msg, "")
			continue
		}
		c.processedOrders[event.OrderID] = true
		c.mutex.Unlock()

		log.Printf("Processing new order: %+v", event)


		confirmation := map[string]interface{}{
			"order_id":  event.OrderID,
			"status":    "confirmed",
			"timestamp": time.Now().UTC(),
			"message":   "Order processed successfully",
		}

		confirmationBytes, err := json.Marshal(confirmation)
		if err != nil {
			errMsg := fmt.Sprintf("failed to marshal confirmation: %v", err)
			log.Printf("Error: %s", errMsg)
			c.publishToDeadLetter(msg.Value, fmt.Errorf(errMsg))
			session.MarkMessage(msg, "")
			continue
		}

		if err := c.publishToTopic("OrderConfirmed", confirmationBytes); err != nil {
			c.publishKPI(KPIEvent{
				KPIName: "order_processing_error",
				Metric:  "publish_error",
				Value:   1,
				Time:    time.Now().Unix(),
			})
			errMsg := fmt.Sprintf("failed to publish order confirmation: %v", err)
			log.Printf("Error: %s", errMsg)
			c.publishToDeadLetter(msg.Value, fmt.Errorf(errMsg))
			session.MarkMessage(msg, "")
			continue
		}

		log.Printf("Successfully processed and confirmed order: %s", event.OrderID)
		c.publishKPI(KPIEvent{
			KPIName: "order_processed",
			Metric:  "success",
			Value:   1,
			Time:    time.Now().Unix(),
		})
		session.MarkMessage(msg, "")
	}

	return nil
}

func (c *InventoryConsumer) publishToTopic(topic string, value []byte) error {
	if c.producer == nil {
		return errors.New("producer not initialized")
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(value),
	}

	partition, offset, err := c.producer.SendMessage(msg)
	if err != nil {
		log.Printf("Failed to send message to topic %s: %v", topic, err)
		return fmt.Errorf("failed to send message to topic %s: %w", topic, err)
	}

	log.Printf("Successfully sent message to topic %s, partition %d, offset %d", topic, partition, offset)
	return nil
}

func (c *InventoryConsumer) publishToDeadLetter(value []byte, originalErr error) {
	errEvent := struct {
		OriginalEvent json.RawMessage `json:"original_event"`
		Error         string          `json:"error"`
		Timestamp     time.Time       `json:"timestamp"`
	}{
		OriginalEvent: value,
		Error:         originalErr.Error(),
		Timestamp:     time.Now().UTC(),
	}

	errBytes, err := json.Marshal(errEvent)
	if err != nil {
		log.Printf("Failed to marshal error event: %v", err)
		return
	}

	if err := c.publishToTopic("DeadLetterQueue", errBytes); err != nil {
		log.Printf("Failed to publish to DeadLetterQueue: %v", err)
	}
}

func (c *InventoryConsumer) Start(cfg *config.Config) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0
	config.Consumer.Return.Errors = true
	config.Producer.Return.Successes = true

	// Configure consumer to start from the earliest offset
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.AutoCommit.Enable = true
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

	config.Producer.Idempotent = true
	config.Net.MaxOpenRequests = 1
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 3

	config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}

	producer, err := sarama.NewSyncProducer(cfg.KafkaBrokers, config)
	if err != nil {
		return fmt.Errorf("failed to create producer: %w", err)
	}
	c.producer = producer

	group, err := sarama.NewConsumerGroup(cfg.KafkaBrokers, cfg.ConsumerGroupID, config)
	if err != nil {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)

	errors := make(chan error)

	go func() {
		log.Printf("Inventory Consumer started, listening to topic: %s", cfg.OrderReceivedTopic)
		for {
			if err := group.Consume(ctx, []string{cfg.OrderReceivedTopic}, c); err != nil {
				errors <- fmt.Errorf("error from consumer: %w", err)
				return
			}

			if ctx.Err() != nil {
				return
			}
		}
	}()

	select {
	case <-sigterm:
		log.Println("Initiating graceful shutdown...")
		cancel()
	case err := <-errors:
		log.Printf("Consumer error: %v", err)
		cancel()
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := group.Close(); err != nil {
			log.Printf("Error closing consumer group: %v", err)
		}
	}()

	if err := c.producer.Close(); err != nil {
		log.Printf("Error closing producer: %v", err)
	}

	wg.Wait()
	log.Println("Consumer successfully closed")
	return nil
}
