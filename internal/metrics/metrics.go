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
)
