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
	"github.com/TohidTonekaboni/LogFlux/internal/tracing"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	logsRawTopic  = "logs.raw"
	logsDLQTopic  = "logs.dlq"
	consumerGroup = "logflux-consumer"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		selfCheck("http://localhost:2113/healthz")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := tracing.Init(ctx, "logflux-consumer", getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"))
	if err != nil {
		log.Fatalf("logflux consumer: tracing init: %v", err)
	}
	defer func() {
		if err := shutdownTracing(context.Background()); err != nil {
			log.Printf("logflux consumer: tracing shutdown: %v", err)
		}
	}()

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
		http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		log.Println("logflux consumer metrics listening on :2113 (/metrics, /healthz)")
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

// selfCheck lets the container's own binary serve as its Docker HEALTHCHECK
// (`consumer healthcheck`), since the runtime image has no shell/curl/wget.
func selfCheck(url string) {
	resp, err := http.Get(url) //nolint:gosec // fixed localhost URL, not user input
	if err != nil || resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
	_ = resp.Body.Close()
	os.Exit(0)
}
