package ingestion

import (
	"io"

	logfluxv1 "github.com/TohidTonekaboni/LogFlux/proto/logflux/v1"
)

// Server implements logfluxv1.LogIngestServer.
type Server struct {
	logfluxv1.UnimplementedLogIngestServer
	Sink Sink
}

func NewServer(sink Sink) *Server {
	return &Server{Sink: sink}
}

func (s *Server) StreamLogs(stream logfluxv1.LogIngest_StreamLogsServer) error {
	var accepted, rejected int64
	for {
		entry, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&logfluxv1.StreamAck{
				AcceptedCount: accepted,
				RejectedCount: rejected,
			})
		}
		if err != nil {
			return err
		}

		if err := Validate(entry); err != nil {
			rejected++
			continue
		}

		if err := s.Sink.Publish(stream.Context(), entry); err != nil {
			return err
		}
		accepted++
	}
}
