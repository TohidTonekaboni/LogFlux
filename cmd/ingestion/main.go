package main

import (
	"log"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/TohidTonekaboni/LogFlux/internal/ingestion"
	"github.com/TohidTonekaboni/LogFlux/internal/kafka"
	logfluxv1 "github.com/TohidTonekaboni/LogFlux/proto/logflux/v1"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

const logsRawTopic = "logs.raw"

func main() {
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

	srv := grpc.NewServer()
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
