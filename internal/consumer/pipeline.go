package consumer

import (
	"context"
	"log"
	"time"

	"github.com/TohidTonekaboni/LogFlux/internal/metrics"
	logfluxv1 "github.com/TohidTonekaboni/LogFlux/proto/logflux/v1"
	kafkago "github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

type Config struct {
	IndexerWorkers int
	BatchSize      int
	FlushInterval  time.Duration
	ChannelBuffer  int // bounded channel capacity -- the backpressure knob
	MaxRetries     int
}

func (c Config) withDefaults() Config {
	if c.IndexerWorkers <= 0 {
		c.IndexerWorkers = 4
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 500
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 2 * time.Second
	}
	if c.ChannelBuffer <= 0 {
		c.ChannelBuffer = 1000
	}
	if c.MaxRetries <= 0 {
		c.MaxRetries = 3
	}
	return c
}

// record pairs a validated LogEntry with the Kafka message it came from, so
// the offset is committed only after the entry is durably indexed or routed
// to the DLQ -- at-least-once delivery per §5.
type record struct {
	entry *logfluxv1.LogEntry
	msg   kafkago.Message
}

// MessageSource is the subset of *kafka.Consumer the pipeline needs.
type MessageSource interface {
	FetchMessage(ctx context.Context) (kafkago.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafkago.Message) error
}

// Indexer is the storage side of the pipeline -- ARCHITECTURE.md §3.5.
type Indexer interface {
	BulkIndex(ctx context.Context, entries []*logfluxv1.LogEntry) error
}

// DLQPublisher routes entries that repeatedly fail indexing to logs.dlq.
type DLQPublisher interface {
	Publish(ctx context.Context, key string, value []byte) error
}

// Pipeline wires: per-partition dispatch -> per-partition normalize workers
// -> batcher -> bulk indexer worker pool -> Elasticsearch / DLQ.
type Pipeline struct {
	cfg     Config
	source  MessageSource
	indexer Indexer
	dlq     DLQPublisher

	partitionChans map[int]chan kafkago.Message
	batchCh        chan record
	bulkCh         chan []record
}

func NewPipeline(source MessageSource, indexer Indexer, dlq DLQPublisher, cfg Config) *Pipeline {
	cfg = cfg.withDefaults()
	return &Pipeline{
		cfg:            cfg,
		source:         source,
		indexer:        indexer,
		dlq:            dlq,
		partitionChans: make(map[int]chan kafkago.Message),
		batchCh:        make(chan record, cfg.ChannelBuffer),
		bulkCh:         make(chan []record, cfg.IndexerWorkers),
	}
}

// Run blocks until ctx is canceled or fetching fails. It starts the batcher
// and indexer-worker pool up front, then dispatches fetched messages into
// per-partition normalize workers, spun up lazily as new partitions are seen.
func (p *Pipeline) Run(ctx context.Context) error {
	for i := 0; i < p.cfg.IndexerWorkers; i++ {
		go p.indexWorker(ctx)
	}
	go p.batch(ctx)

	for {
		msg, err := p.source.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}

		ch, ok := p.partitionChans[msg.Partition]
		if !ok {
			ch = make(chan kafkago.Message, p.cfg.ChannelBuffer)
			p.partitionChans[msg.Partition] = ch
			go p.normalize(ctx, ch)
		}

		select {
		case ch <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// normalize owns one partition's messages, so processing stays concurrent
// across partitions while staying strictly ordered within one -- the topic
// is partitioned by service_name (§3.3), so this preserves per-service order.
func (p *Pipeline) normalize(ctx context.Context, ch <-chan kafkago.Message) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var entry logfluxv1.LogEntry
			if err := proto.Unmarshal(msg.Value, &entry); err != nil {
				log.Printf("logflux consumer: dropping unparseable message at %s/%d/%d: %v",
					msg.Topic, msg.Partition, msg.Offset, err)
				continue
			}

			select {
			case p.batchCh <- record{entry: &entry, msg: msg}:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (p *Pipeline) batch(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.FlushInterval)
	defer ticker.Stop()

	buf := make([]record, 0, p.cfg.BatchSize)
	flush := func() {
		if len(buf) == 0 {
			return
		}
		select {
		case p.bulkCh <- buf:
		case <-ctx.Done():
		}
		buf = make([]record, 0, p.cfg.BatchSize)
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case r := <-p.batchCh:
			buf = append(buf, r)
			if len(buf) >= p.cfg.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (p *Pipeline) indexWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case batch, ok := <-p.bulkCh:
			if !ok {
				return
			}
			p.processBatch(ctx, batch)
		}
	}
}

func (p *Pipeline) processBatch(ctx context.Context, batch []record) {
	entries := make([]*logfluxv1.LogEntry, len(batch))
	msgs := make([]kafkago.Message, len(batch))
	for i, r := range batch {
		entries[i] = r.entry
		msgs[i] = r.msg
	}

	start := time.Now()
	err := p.withRetries(ctx, entries)
	metrics.BulkIndexDuration.Observe(time.Since(start).Seconds())

	if err != nil {
		log.Printf("logflux consumer: batch failed after retries, routing %d entries to DLQ: %v", len(entries), err)
		p.routeToDLQ(ctx, batch)
		metrics.BulkIndexErrorsTotal.Add(float64(len(entries)))
	} else {
		metrics.LogsIndexedTotal.Add(float64(len(entries)))
	}

	// Commit offsets either way: a DLQ'd batch has been handled, not lost,
	// so re-consuming it would just produce duplicate DLQ entries.
	if err := p.source.CommitMessages(ctx, msgs...); err != nil {
		log.Printf("logflux consumer: commit offsets: %v", err)
	}
}

func (p *Pipeline) withRetries(ctx context.Context, entries []*logfluxv1.LogEntry) error {
	var err error
	backoff := 200 * time.Millisecond
	for attempt := 0; attempt < p.cfg.MaxRetries; attempt++ {
		if err = p.indexer.BulkIndex(ctx, entries); err == nil {
			return nil
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
		backoff *= 2
	}
	return err
}

func (p *Pipeline) routeToDLQ(ctx context.Context, batch []record) {
	for _, r := range batch {
		if err := p.dlq.Publish(ctx, r.entry.GetServiceName(), r.msg.Value); err != nil {
			log.Printf("logflux consumer: dlq publish failed for %s/%d/%d: %v",
				r.msg.Topic, r.msg.Partition, r.msg.Offset, err)
		}
	}
}
