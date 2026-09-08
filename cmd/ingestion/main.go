package main

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/TohidTonekaboni/LogFlux/internal/ingestion"
	"github.com/TohidTonekaboni/LogFlux/internal/kafka"
	"github.com/TohidTonekaboni/LogFlux/internal/tracing"
	logfluxv1 "github.com/TohidTonekaboni/LogFlux/proto/logflux/v1"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

const logsRawTopic = "logs.raw"

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		selfCheck("http://localhost:2112/healthz")
	}

	ctx := context.Background()
	shutdownTracing, err := tracing.Init(ctx, "logflux-ingestion", getEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"))
	if err != nil {
		log.Fatalf("logflux ingestion: tracing init: %v", err)
	}
	defer func() {
		if err := shutdownTracing(context.Background()); err != nil {
			log.Printf("logflux ingestion: tracing shutdown: %v", err)
		}
	}()

	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9094,localhost:9095"), ",")

	producer := kafka.NewProducer(brokers, logsRawTopic)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("logflux ingestion: kafka producer close: %v", err)
		}
	}()

	lis, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("logflux ingestion: listen: %v", err)
	}

	srv := grpc.NewServer(grpc.StatsHandler(otelgrpc.NewServerHandler()))
	logfluxv1.RegisterLogIngestServer(srv, ingestion.NewServer(ingestion.NewKafkaSink(producer)))

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		http.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		log.Println("logflux ingestion metrics listening on :2112 (/metrics, /healthz)")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Printf("logflux ingestion: metrics server: %v", err)
		}
	}()

	log.Println("logflux ingestion server listening on :9000")
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("logflux ingestion: serve: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// selfCheck lets the container's own binary serve as its Docker HEALTHCHECK
// (`ingestion healthcheck`), since the runtime image has no shell/curl/wget.
func selfCheck(url string) {
	resp, err := http.Get(url) //nolint:gosec // fixed localhost URL, not user input
	if err != nil || resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
	_ = resp.Body.Close()
	os.Exit(0)
}
