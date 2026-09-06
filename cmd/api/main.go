package main

import (
	"log"
	"os"

	"github.com/TohidTonekaboni/LogFlux/internal/api"
	"github.com/TohidTonekaboni/LogFlux/internal/esclient"
)

func main() {
	esClient, err := esclient.New(
		[]string{getEnv("ELASTICSEARCH_URL", "http://localhost:9200")},
		getEnv("ES_USER", ""),
		getEnv("ES_PASSWORD", ""),
	)
	if err != nil {
		log.Fatalf("logflux api: elasticsearch client: %v", err)
	}

	router := api.New(esClient).Routes()

	addr := ":" + getEnv("API_PORT", "8081")
	log.Printf("logflux REST API listening on %s", addr)
	if err := router.Run(addr); err != nil {
		log.Fatalf("logflux api: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
