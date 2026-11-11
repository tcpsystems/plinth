// Package telemetry supplies opt-in OpenTelemetry traces, HTTP metrics and
// trace-correlated slog logging. Call Setup once before constructing middleware.
package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"os"

	"github.com/tcpsystems/plinth/buildinfo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	metricapi "go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	traceapi "go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// Options identifies a service and controls trace sampling.
type Options struct {
	// ServiceName is required when export is enabled.
	ServiceName string
	// ServiceVersion defaults to buildinfo.Get().ModuleVersion.
	ServiceVersion string
	// SampleRatio ranges from zero (no root spans) to one (all root spans).
	// Children respect their parent's sampling decision.
	SampleRatio float64
}

// Setup installs global providers and W3C TraceContext propagation. Exporters
// read standard OTEL_EXPORTER_OTLP_* variables, including per-signal endpoints
// and headers. A signal without an explicit endpoint uses a no-op provider;
// with no endpoints Setup succeeds without starting exporters.
// Call the returned function with a live context on exit to flush both signals.
// Setup is intended for startup, before serving requests.
func Setup(ctx context.Context, opts Options) (func(context.Context) error, error) {
	general := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != ""
	traces := general || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != ""
	metrics := general || os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") != ""
	if traces || metrics {
		if opts.ServiceName == "" {
			return nil, errors.New("telemetry: service name is required")
		}
		if math.IsNaN(opts.SampleRatio) || opts.SampleRatio < 0 || opts.SampleRatio > 1 {
			return nil, errors.New("telemetry: sampling ratio must be between zero and one")
		}
	}
	if opts.ServiceVersion == "" {
		opts.ServiceVersion = buildinfo.Get().ModuleVersion
	}
	res := resource.NewWithAttributes(semconv.SchemaURL, semconv.ServiceName(opts.ServiceName), semconv.ServiceVersion(opts.ServiceVersion))
	var stopTrace, stopMetric = func(context.Context) error { return nil }, func(context.Context) error { return nil }
	var tp traceapi.TracerProvider = tracenoop.NewTracerProvider()
	var mp metricapi.MeterProvider = metricnoop.NewMeterProvider()
	if traces {
		exporter, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, err
		}
		provider := sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(opts.SampleRatio))), sdktrace.WithBatcher(exporter))
		tp, stopTrace = provider, provider.Shutdown
	}
	if metrics {
		exporter, err := otlpmetrichttp.New(ctx)
		if err != nil {
			_ = stopTrace(ctx)
			return nil, err
		}
		provider := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)))
		mp, stopMetric = provider, provider.Shutdown
	}
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	return func(ctx context.Context) error { return errors.Join(stopMetric(ctx), stopTrace(ctx)) }, nil
}

// Logger adds trace_id and span_id from each record's context. A nil base uses
// slog.Default(). Use InfoContext and the other context-aware slog methods.
func Logger(base *slog.Logger) *slog.Logger {
	if base == nil {
		base = slog.Default()
	}
	return slog.New(&traceHandler{inner: base.Handler()})
}
