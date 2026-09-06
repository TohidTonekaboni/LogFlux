package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	LogsReceivedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name : "logs_received_total",
		Help : "Total number of log entries accepted by the ingestion server.",
	})
	LogsRejectedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "logs_rejected_total",
		Help: "Total number of log entries rejected by validation.",
	})

	KafkaPublishDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "kafka_publish_duration_seconds",
		Help:    "Latency of publishing a single log entry to Kafka.",
		Buckets: prometheus.DefBuckets,
	})

	GRPCStreamActiveConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "grpc_stream_active_connections",
		Help: "Number of currently open StreamLogs gRPC streams.",
	})

	BulkIndexDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "bulk_index_duration_seconds",
		Help:    "Latency of a single bulk-index call to the indexer, including retries.",
		Buckets: prometheus.DefBuckets,
	})
	BulkIndexErrorsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "bulk_index_errors_total",
		Help: "Total number of log entries that failed bulk indexing after all retries and were routed to the DLQ.",
	})
	LogsIndexedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "logs_indexed_total",
		Help: "Total number of log entries successfully bulk-indexed.",
	})

	ConsumerLag = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "consumer_lag",
		Help: "Number of messages the consumer group is behind on logs.raw.",
	})
)
