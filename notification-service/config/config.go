package config

import (
	"errors"
	"os"
)

// Config holds the configuration for the notification service
type Config struct {
	KafkaBrokers []string
	HTTPPort     string
}

// New creates a new configuration with values from environment variables or defaults
func New() (*Config, error) {
	config := &Config{
		KafkaBrokers: []string{"localhost:9092"},
		HTTPPort:     "8084",
	}

	// Override with environment variables if present
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		config.KafkaBrokers = []string{brokers}
	}

	if port := os.Getenv("HTTP_PORT"); port != "" {
		config.HTTPPort = port
	}

	// Validate configuration
	if len(config.KafkaBrokers) == 0 {
		return nil, errors.New("no Kafka brokers specified")
	}

	return config, nil
}
