package main

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/prachi/kafka-ecommerce/notification-service/config"
	"github.com/prachi/kafka-ecommerce/notification-service/consumer"
)

func main() {
	log.Println("Starting Notification Service...")

	cfg, err := config.New()
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	notificationConsumer := consumer.NewNotificationConsumer(cfg.KafkaBrokers)

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

	errChan := make(chan error, 1)
	go func() {
		if err := notificationConsumer.Start(); err != nil {
			errChan <- err
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errChan:
		log.Fatalf("Consumer error: %v", err)
	case sig := <-sigChan:
		log.Printf("Received signal %v, shutting down...", sig)
	}

	log.Println("Notification service stopped")
}
