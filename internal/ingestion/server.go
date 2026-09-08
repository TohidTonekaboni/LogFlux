package ingestion

import (
	"io"

	"github.com/TohidTonekaboni/LogFlux/internal/metrics"
	"github.com/TohidTonekaboni/LogFlux/internal/tracing"
	logfluxv1 "github.com/TohidTonekaboni/LogFlux/proto/logflux/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

var tracer = tracing.Tracer("logflux/ingestion")

type Server struct {
	logfluxv1.UnimplementedLogIngestServer
	Sink Sink
}

func NewServer(sink Sink) *Server {
	return &Server{Sink: sink}
}

func (s *Server) StreamLogs(stream logfluxv1.LogIngest_StreamLogsServer) error {
	metrics.GRPCStreamActiveConnections.Inc()
	defer metrics.GRPCStreamActiveConnections.Dec()

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

		entryCtx, span := tracer.Start(stream.Context(), "ingestion.process_entry",
			trace.WithAttributes(attribute.String("logflux.service_name", entry.GetServiceName())))

		if err := Validate(entry); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.End()
			rejected++
			metrics.LogsRejectedTotal.Inc()
			continue
		}

		if err := s.Sink.Publish(entryCtx, entry); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.End()
			return err
		}
		span.End()
		accepted++
		metrics.LogsReceivedTotal.Inc()
	}
}
