package config

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
	// "github.com/prachi/kafka-ecommerce/inventory-service/config"
)

type Config struct {
	KafkaBrokers         []string `envconfig:"KAFKA_BROKERS" required:"true" default:"localhost:9092"`
	ConsumerGroupID      string   `envconfig:"CONSUMER_GROUP_ID" default:"inventory-service"`
	OrderReceivedTopic   string   `envconfig:"ORDER_RECEIVED_TOPIC" default:"OrderReceived"`
	OrderConfirmedTopic  string   `envconfig:"ORDER_CONFIRMED_TOPIC" default:"OrderConfirmed"`
	DeadLetterQueueTopic string   `envconfig:"DEAD_LETTER_QUEUE_TOPIC" default:"DeadLetterQueue"`
}

func New() (*Config, error) {
	var cfg Config
	if err := envconfig.Process("", &cfg); err != nil {
		return nil, fmt.Errorf("failed to process config: %w", err)
	}
	return &cfg, nil
}
