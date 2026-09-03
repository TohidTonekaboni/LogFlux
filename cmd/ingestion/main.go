package main

import (
	"log"
	"net"

	"github.com/TohidTonekaboni/LogFlux/internal/ingestion"
	logfluxv1 "github.com/TohidTonekaboni/LogFlux/proto/logflux/v1"
	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatalf("logflux ingestion: listen: %v", err)
	}

	srv := grpc.NewServer()
	logfluxv1.RegisterLogIngestServer(srv, ingestion.NewServer(ingestion.LogSink{}))

	log.Println("logflux ingestion server listening on :9000")
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("logflux ingestion: serve: %v", err)
	}
}
