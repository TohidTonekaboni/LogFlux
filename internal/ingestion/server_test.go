package ingestion

import (
	"context"
	"net"
	"testing"
	"time"

	logfluxv1 "github.com/TohidTonekaboni/LogFlux/proto/logflux/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func dialServer(t *testing.T, sink Sink) logfluxv1.LogIngestClient {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	logfluxv1.RegisterLogIngestServer(srv, NewServer(sink))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return logfluxv1.NewLogIngestClient(conn)
}

func TestServer_StreamLogs(t *testing.T) {
	sink := NewMemorySink()
	client := dialServer(t, sink)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.StreamLogs(ctx)
	if err != nil {
		t.Fatalf("StreamLogs: %v", err)
	}

	valid := &logfluxv1.LogEntry{
		ServiceName:     "svc",
		TimestampUnixMs: 1,
		Level:           logfluxv1.Level_INFO,
		Message:         "hello",
	}
	invalid := &logfluxv1.LogEntry{ServiceName: "svc"} // zero timestamp, unspecified level, empty message

	if err := stream.Send(valid); err != nil {
		t.Fatalf("Send(valid): %v", err)
	}
	if err := stream.Send(invalid); err != nil {
		t.Fatalf("Send(invalid): %v", err)
	}

	ack, err := stream.CloseAndRecv()
	if err != nil {
		t.Fatalf("CloseAndRecv: %v", err)
	}
	if ack.AcceptedCount != 1 || ack.RejectedCount != 1 {
		t.Errorf("ack = %+v, want accepted=1 rejected=1", ack)
	}

	entries := sink.Entries()
	if len(entries) != 1 || entries[0].GetMessage() != "hello" {
		t.Errorf("sink entries = %+v, want exactly the valid entry", entries)
	}
}
