package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prachi/kafka-ecommerce/warehouse-service/config"
	"github.com/prachi/kafka-ecommerce/warehouse-service/consumer"
)

// These types have been moved to the consumer package

func main() {
	log.Println("Starting Warehouse Service...")

	// Load configuration
	cfg, err := config.New()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Create warehouse consumer
	warehouseConsumer := consumer.NewWarehouseConsumer(cfg.KafkaBrokers)

	// Start HTTP server for health check
	go func() {
		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		})
		log.Printf("HTTP server listening on :%s (/health)", cfg.HTTPPort)
		if err := http.ListenAndServe(":"+cfg.HTTPPort, nil); err != nil {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Start consumer in a goroutine
	errChan := make(chan error, 1)
	go func() {
		if err := warehouseConsumer.Start(); err != nil {
			errChan <- err
		}
	}()

	// Wait for interrupt signal to gracefully shut down
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errChan:
		log.Fatalf("Consumer error: %v", err)
	case sig := <-sigChan:
		log.Printf("Received signal %v, shutting down...", sig)
	}

	log.Println("Warehouse service stopped")
}

// This function has been moved to the consumer package

// These functions have been moved to the consumer package
