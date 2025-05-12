package config

import (
	"errors"
	"os"
)

type Config struct {
	KafkaBrokers []string
	HTTPPort     string
}

func New() (*Config, error) {
	config := &Config{
		KafkaBrokers: []string{"localhost:9092"},
		HTTPPort:     "8084",
	}

	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		config.KafkaBrokers = []string{brokers}
	}

	if port := os.Getenv("HTTP_PORT"); port != "" {
		config.HTTPPort = port
	}

	if len(config.KafkaBrokers) == 0 {
		return nil, errors.New("no Kafka brokers specified")
	}

	return config, nil
}
