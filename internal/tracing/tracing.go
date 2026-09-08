// Package tracing provides shared OpenTelemetry tracer setup for ingestion
// and consumer, so a trace started when the ingestion server receives a log
// entry can be continued (via Kafka message headers) when the consumer
// indexes it -- ARCHITECTURE.md Phase 7.
package tracing

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// Init configures the global tracer provider to export spans via OTLP/gRPC
// to otlpEndpoint (e.g. "jaeger:4317"), tagged with serviceName. It returns a
// shutdown func that flushes and closes the exporter; callers must call it
// before the process exits so buffered spans aren't lost.
func Init(ctx context.Context, serviceName, otlpEndpoint string) (shutdown func(context.Context) error, err error) {
	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(otlpEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(serviceName)),
	)
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp.Shutdown, nil
}

// Tracer returns a named tracer via the global provider Init configured.
func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}
