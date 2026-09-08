package main

import (
	"log"
	"net/http"
	"os"

	"github.com/TohidTonekaboni/LogFlux/internal/api"
	"github.com/TohidTonekaboni/LogFlux/internal/esclient"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		selfCheck("http://localhost:" + getEnv("API_PORT", "8081") + "/healthz")
	}

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

// selfCheck lets the container's own binary serve as its Docker HEALTHCHECK
// (`api healthcheck`), since the runtime image has no shell/curl/wget.
func selfCheck(url string) {
	resp, err := http.Get(url) //nolint:gosec // fixed localhost URL, not user input
	if err != nil || resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
	_ = resp.Body.Close()
	os.Exit(0)
}
