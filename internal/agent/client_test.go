package agent

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	logfluxv1 "github.com/TohidTonekaboni/LogFlux/proto/logflux/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type fakeServer struct {
	logfluxv1.UnimplementedLogIngestServer
	received chan struct{}
}

func newFakeServer() *fakeServer {
	return &fakeServer{received: make(chan struct{}, 1000)}
}

func (f *fakeServer) StreamLogs(stream logfluxv1.LogIngest_StreamLogsServer) error {
	var n int64
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&logfluxv1.StreamAck{AcceptedCount: n})
		}
		if err != nil {
			return err
		}
		n++
		f.received <- struct{}{}
	}
}

func dialFakeServer(t *testing.T) (*fakeServer, *grpc.ClientConn) {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	fs := newFakeServer()
	logfluxv1.RegisterLogIngestServer(srv, fs)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return fs, conn
}

func recvN(t *testing.T, ch <-chan struct{}, n int, within time.Duration) {
	t.Helper()
	deadline := time.After(within)
	for i := 0; i < n; i++ {
		select {
		case <-ch:
		case <-deadline:
			t.Fatalf("timed out after receiving %d/%d", i, n)
		}
	}
}

func TestClient_FlushesOnBatchSize(t *testing.T) {
	fs, conn := dialFakeServer(t)
	c := &Client{
		cfg:    Config{BatchSize: 3, FlushInterval: time.Hour, QueueCapacity: 10}.withDefaults(),
		client: logfluxv1.NewLogIngestClient(conn),
		queue:  make(chan *logfluxv1.LogEntry, 10),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	for i := 0; i < 3; i++ {
		c.Enqueue(&logfluxv1.LogEntry{Message: "x"})
	}

	recvN(t, fs.received, 3, time.Second)
}

func TestClient_FlushesOnInterval(t *testing.T) {
	fs, conn := dialFakeServer(t)
	c := &Client{
		cfg:    Config{BatchSize: 100, FlushInterval: 50 * time.Millisecond, QueueCapacity: 10}.withDefaults(),
		client: logfluxv1.NewLogIngestClient(conn),
		queue:  make(chan *logfluxv1.LogEntry, 10),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx) }()

	c.Enqueue(&logfluxv1.LogEntry{Message: "x"})

	recvN(t, fs.received, 1, time.Second)
}

func TestClient_EnqueueDropsOldestWhenFull(t *testing.T) {
	c := &Client{
		cfg:   Config{QueueCapacity: 2}.withDefaults(),
		queue: make(chan *logfluxv1.LogEntry, 2),
	}

	for i := 1; i <= 3; i++ {
		c.Enqueue(&logfluxv1.LogEntry{Message: fmt.Sprintf("entry-%d", i)})
	}

	var got []string
	for {
		select {
		case e := <-c.queue:
			got = append(got, e.GetMessage())
		default:
			if len(got) != 2 || got[0] != "entry-2" || got[1] != "entry-3" {
				t.Errorf("queue contents = %v, want [entry-2 entry-3]", got)
			}
			return
		}
	}
}
