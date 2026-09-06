package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/TohidTonekaboni/LogFlux/internal/consumer"
	"github.com/TohidTonekaboni/LogFlux/internal/esclient"
	"github.com/TohidTonekaboni/LogFlux/internal/kafka"
	"github.com/TohidTonekaboni/LogFlux/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	logsRawTopic  = "logs.raw"
	logsDLQTopic  = "logs.dlq"
	consumerGroup = "logflux-consumer"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9094,localhost:9095"), ",")

	kafkaConsumer := kafka.NewConsumer(brokers, logsRawTopic, consumerGroup)
	defer closeLogged("kafka consumer", kafkaConsumer.Close)

	dlqProducer := kafka.NewProducer(brokers, logsDLQTopic)
	defer closeLogged("dlq producer", dlqProducer.Close)

	esClient, err := esclient.New(
		[]string{getEnv("ELASTICSEARCH_URL", "http://localhost:9200")},
		getEnv("ES_USER", ""),
		getEnv("ES_PASSWORD", ""),
	)
	if err != nil {
		log.Fatalf("logflux consumer: elasticsearch client: %v", err)
	}

	pipeline := consumer.NewPipeline(kafkaConsumer, esClient, dlqProducer, consumer.Config{})

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("logflux consumer metrics listening on :2113/metrics")
		if err := http.ListenAndServe(":2113", nil); err != nil {
			log.Printf("logflux consumer: metrics server: %v", err)
		}
	}()

	go monitorLag(ctx, kafkaConsumer)

	log.Println("logflux consumer running")
	if err := pipeline.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("logflux consumer: %v", err)
	}
}

func monitorLag(ctx context.Context, c *kafka.Consumer) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			metrics.ConsumerLag.Set(float64(c.Lag()))
		}
	}
}

func closeLogged(name string, close func() error) {
	if err := close(); err != nil {
		log.Printf("logflux consumer: close %s: %v", name, err)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
