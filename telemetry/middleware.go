package telemetry

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Middleware returns an HTTP middleware that creates a server span for each
// request. It extracts W3C trace context from incoming headers and sets span
// attributes for method, route, status code, and errors.
//
// Place this middleware early in the chain, after reqid.Middleware but before
// handlers that should be traced.
func Middleware(serviceName string) func(http.Handler) http.Handler {
	tracer := otel.Tracer(serviceName)
	meter := otel.Meter(serviceName)
	count, err := meter.Int64Counter("http.server.request.count", metric.WithUnit("{request}"))
	if err != nil {
		otel.Handle(err)
	}
	duration, err := meter.Float64Histogram("http.server.request.duration", metric.WithUnit("s"), metric.WithExplicitBucketBoundaries(.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10))
	if err != nil {
		otel.Handle(err)
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Extract trace context from headers
			start := time.Now()
			ctx := propagation.TraceContext{}.Extract(r.Context(), propagation.HeaderCarrier(r.Header))

			// The matched template becomes available after ServeMux returns.
			spanName := r.Method
			if route := getRoute(r); route != "" {
				spanName = fmt.Sprintf("%s %s", r.Method, route)
			}

			// Start server span
			ctx, span := tracer.Start(ctx, spanName,
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(
					semconv.HTTPRequestMethodKey.String(r.Method),
				),
			)
			defer span.End()

			// Update request with new context
			r = r.WithContext(ctx)

			// Wrap response writer to capture status
			sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
			completed := false
			defer func() {
				status := sw.status
				if !completed {
					status = http.StatusInternalServerError
				}
				route := getRoute(r)
				if route != "" {
					span.SetName(r.Method + " " + route)
				}
				span.SetAttributes(semconv.HTTPRoute(route), semconv.HTTPResponseStatusCode(status))
				if status >= 500 {
					span.SetStatus(codes.Error, http.StatusText(status))
				}
				attrs := metric.WithAttributes(semconv.HTTPRoute(route), semconv.HTTPRequestMethodKey.String(requestMethod(r.Method)), attribute.String("http.response.status_class", fmt.Sprintf("%dxx", status/100)))
				if count != nil {
					count.Add(ctx, 1, attrs)
				}
				if duration != nil {
					duration.Record(ctx, time.Since(start).Seconds(), attrs)
				}
			}()

			// Call next handler
			next.ServeHTTP(sw, r)
			completed = true
		})
	}
}

// statusWriter wraps http.ResponseWriter to capture the status code.
type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	if code >= 200 || code == http.StatusSwitchingProtocols {
		w.status = code
		w.wroteHeader = true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}

func (w *statusWriter) FlushError() error {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}
func (w *statusWriter) Flush() { _ = w.FlushError() }
func (w *statusWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(w.ResponseWriter).Hijack()
}

// getRoute reads only the ServeMux template, after routing has completed.
func getRoute(r *http.Request) string {
	_, route, ok := strings.Cut(r.Pattern, " ")
	if !ok {
		route = r.Pattern
	}
	return route
}

func requestMethod(method string) string {
	switch method {
	case "GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PATCH":
		return method
	default:
		return "_OTHER"
	}
}

// ClientTransport returns an http.RoundTripper that creates client spans for
// outgoing HTTP requests. It injects W3C trace context into outgoing headers.
//
// Use this with http.Client{Transport: telemetry.ClientTransport("my-service", nil)}
// to propagate traces to downstream services.
func ClientTransport(serviceName string, base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	tracer := otel.Tracer(serviceName)
	return &clientTransport{tracer: tracer, base: base}
}

type clientTransport struct {
	tracer trace.Tracer
	base   http.RoundTripper
}

func (t *clientTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	// Start client span
	spanName := r.Method
	if r.URL.Host != "" {
		spanName = fmt.Sprintf("%s %s", r.Method, r.URL.Host)
	}

	ctx, span := t.tracer.Start(r.Context(), spanName,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.HTTPRequestMethodKey.String(r.Method),
			semconv.ServerAddress(r.URL.Hostname()),
		),
	)

	// Inject trace context into outgoing headers
	r = r.Clone(ctx)
	if r.Header == nil {
		r.Header = make(http.Header)
	}
	propagation.TraceContext{}.Inject(ctx, propagation.HeaderCarrier(r.Header))

	// Perform request
	resp, err := t.base.RoundTrip(r)

	// Record response attributes
	if resp != nil {
		span.SetAttributes(
			semconv.HTTPResponseStatusCode(resp.StatusCode),
		)
		if resp.StatusCode >= 400 {
			span.SetStatus(codes.Error, fmt.Sprintf("HTTP %d", resp.StatusCode))
		}
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	if err != nil || resp == nil || resp.Body == nil {
		span.End()
	} else {
		resp.Body = &spanBody{ReadCloser: resp.Body, end: span.End}
	}

	return resp, err
}

type spanBody struct {
	io.ReadCloser
	once sync.Once
	end  func(...trace.SpanEndOption)
}

func (b *spanBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.once.Do(func() { b.end() })
	}
	return n, err
}
func (b *spanBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(func() { b.end() })
	return err
}

// TraceIDFromContext returns the trace ID from the active span in ctx, or "".
func TraceIDFromContext(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if !span.SpanContext().IsValid() {
		return ""
	}
	return span.SpanContext().TraceID().String()
}

// SpanIDFromContext returns the span ID from the active span in ctx, or "".
func SpanIDFromContext(ctx context.Context) string {
	span := trace.SpanFromContext(ctx)
	if !span.SpanContext().IsValid() {
		return ""
	}
	return span.SpanContext().SpanID().String()
}
