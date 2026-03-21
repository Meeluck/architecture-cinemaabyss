package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	kafkaClient, err := NewKafkaClient(cfg.KafkaBrokers)
	if err != nil {
		log.Fatalf("kafka init error: %v", err)
	}
	defer kafkaClient.Close()

	if err := kafkaClient.StartConsumers(); err != nil {
		log.Fatalf("start consumers error: %v", err)
	}

	mux := http.NewServeMux()
	handler := NewHTTPHandler(kafkaClient)
	handler.Register(mux)

	server := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           LoggingMiddleware(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("events-service started on :%s", cfg.Port)
		log.Printf("kafka brokers=%v", cfg.KafkaBrokers)

		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("http server error: %v", err)
		}
	}()

	waitForShutdown(server)
}

func waitForShutdown(server *http.Server) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Printf("shutdown signal received")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Printf("http shutdown error: %v", err)
	}
}
