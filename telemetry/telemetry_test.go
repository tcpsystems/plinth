package telemetry

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tcpsystems/plinth/buildinfo"
	"github.com/tcpsystems/plinth/httpx"
	"github.com/tcpsystems/plinth/mw"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func isolate(t *testing.T) {
	t.Helper()
	tp, mp, prop := otel.GetTracerProvider(), otel.GetMeterProvider(), otel.GetTextMapPropagator()
	t.Cleanup(func() { otel.SetTracerProvider(tp); otel.SetMeterProvider(mp); otel.SetTextMapPropagator(prop) })
	for _, key := range []string{"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "OTEL_EXPORTER_OTLP_HEADERS", "OTEL_EXPORTER_OTLP_TRACES_HEADERS", "OTEL_EXPORTER_OTLP_METRICS_HEADERS"} {
		t.Setenv(key, "")
	}
}

func TestSetupNoEndpoint(t *testing.T) {
	isolate(t)
	stop, err := Setup(context.Background(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, span := otel.Tracer("test").Start(context.Background(), "disabled")
	defer span.End()
	if span.IsRecording() || trace.SpanContextFromContext(ctx).IsValid() {
		t.Fatal("no-op span expected")
	}
	if err := stop(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestSetupValidation(t *testing.T) {
	isolate(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:1")
	for _, opts := range []Options{{}, {ServiceName: "test", SampleRatio: -1}, {ServiceName: "test", SampleRatio: 2}, {ServiceName: "test", SampleRatio: math.NaN()}} {
		if _, err := Setup(context.Background(), opts); err == nil {
			t.Fatalf("accepted %+v", opts)
		}
	}
}

func providers(t *testing.T) (*tracetest.InMemoryExporter, *sdkmetric.ManualReader) {
	t.Helper()
	isolate(t)
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()); _ = tp.Shutdown(context.Background()) })
	return exp, reader
}

func TestHTTPPropagationMetricsAndLogs(t *testing.T) {
	exp, reader := providers(t)
	var logs strings.Builder
	logger := Logger(slog.New(slog.NewJSONHandler(&logs, nil))).With("service", "test").WithGroup("request")
	mux := http.NewServeMux()
	mux.HandleFunc("GET /items/{id}", func(w http.ResponseWriter, r *http.Request) {
		logger.InfoContext(r.Context(), "received")
		w.WriteHeader(103)
		httpx.WriteJSON(w, 201, map[string]string{"id": r.PathValue("id")})
	})
	server := httptest.NewServer(mw.Chain(mux, Middleware("test"), mw.Recover(logger)))
	defer server.Close()
	client := &http.Client{Transport: ClientTransport("test", nil)}
	ctx, root := otel.Tracer("test").Start(context.Background(), "root")
	req, err := http.NewRequestWithContext(ctx, "GET", server.URL+"/items/private-id?secret=hidden", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(io.Discard, resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	root.End()
	if req.Header.Get("traceparent") != "" {
		t.Fatal("original headers mutated")
	}
	spans := exp.GetSpans()
	if len(spans) != 3 {
		t.Fatalf("spans: %d", len(spans))
	}
	var serverSpan, clientSpan tracetest.SpanStub
	for _, span := range spans {
		if span.SpanKind == trace.SpanKindServer {
			serverSpan = span
		}
		if span.SpanKind == trace.SpanKindClient {
			clientSpan = span
		}
		for _, a := range span.Attributes {
			if strings.Contains(a.Value.AsString(), "private-id") || strings.Contains(a.Value.AsString(), "hidden") {
				t.Fatal("raw URL leaked", a)
			}
		}
	}
	if serverSpan.Name != "GET /items/{id}" || serverSpan.Parent.SpanID() != clientSpan.SpanContext.SpanID() || clientSpan.Parent.SpanID() != root.SpanContext().SpanID() || serverSpan.SpanContext.TraceID() != root.SpanContext().TraceID() {
		t.Fatalf("broken span chain: %+v", spans)
	}
	assertAttr(t, serverSpan.Attributes, "http.route", "/items/{id}")
	var record map[string]any
	if err := json.Unmarshal([]byte(logs.String()), &record); err != nil {
		t.Fatal(err)
	}
	fields := record["request"].(map[string]any)
	if fields["trace_id"] != serverSpan.SpanContext.TraceID().String() || fields["span_id"] != serverSpan.SpanContext.SpanID().String() {
		t.Fatal(record)
	}
	var data metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	seenCount, seenDuration := false, false
	for _, scope := range data.ScopeMetrics {
		for _, m := range scope.Metrics {
			switch d := m.Data.(type) {
			case metricdata.Sum[int64]:
				seenCount = true
				if len(d.DataPoints) != 1 || d.DataPoints[0].Value != 1 {
					t.Fatal(d)
				}
				attrs := d.DataPoints[0].Attributes.ToSlice()
				assertAttr(t, attrs, "http.route", "/items/{id}")
				assertAttr(t, attrs, "http.request.method", "GET")
				assertAttr(t, attrs, "http.response.status_class", "2xx")
			case metricdata.Histogram[float64]:
				seenDuration = true
				if len(d.DataPoints) != 1 || d.DataPoints[0].Count != 1 || d.DataPoints[0].Sum <= 0 {
					t.Fatal(d)
				}
			}
		}
	}
	if !seenCount || !seenDuration {
		t.Fatal("missing metrics", data)
	}
}

func assertAttr(t *testing.T, attrs []attribute.KeyValue, key, value string) {
	t.Helper()
	for _, a := range attrs {
		if string(a.Key) == key && a.Value.AsString() == value {
			return
		}
	}
	t.Fatalf("missing %s=%s in %v", key, value, attrs)
}

func TestShutdownFlushesOTLP(t *testing.T) {
	for _, specific := range []bool{false, true} {
		t.Run(map[bool]string{false: "general", true: "per-signal"}[specific], func(t *testing.T) {
			isolate(t)
			var mu sync.Mutex
			received := map[string]string{}
			collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil || len(body) == 0 {
					t.Error("empty export", err)
				}
				mu.Lock()
				received[r.URL.Path] = r.Header.Get("x-test")
				mu.Unlock()
				w.Header().Set("Content-Type", "application/x-protobuf")
				w.WriteHeader(200)
			}))
			defer collector.Close()
			tracePath, metricPath := "/v1/traces", "/v1/metrics"
			t.Setenv("OTEL_EXPORTER_OTLP_HEADERS", "x-test=general")
			if specific {
				tracePath, metricPath = "/custom/traces", "/custom/metrics"
				t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", collector.URL+tracePath)
				t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", collector.URL+metricPath)
				t.Setenv("OTEL_EXPORTER_OTLP_TRACES_HEADERS", "x-test=trace")
				t.Setenv("OTEL_EXPORTER_OTLP_METRICS_HEADERS", "x-test=metric")
			} else {
				t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.URL)
			}
			stop, err := Setup(context.Background(), Options{ServiceName: "test", SampleRatio: 1})
			if err != nil {
				t.Fatal(err)
			}
			tp := otel.GetTracerProvider().(*sdktrace.TracerProvider)
			_, span := tp.Tracer("test").Start(context.Background(), "flush")
			span.End()
			// The SDK in-memory recorder verifies resource identity independently of wire encoding.
			recorder := tracetest.NewSpanRecorder()
			tp.RegisterSpanProcessor(recorder)
			_, span = tp.Tracer("test").Start(context.Background(), "resource")
			span.End()
			attrs := recorder.Ended()[0].Resource().Set()
			version, _ := attrs.Value(attribute.Key("service.version"))
			if version.AsString() != buildinfo.Get().ModuleVersion {
				t.Fatal("wrong build version", version)
			}
			Middleware("test")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := stop(ctx); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			wantTrace, wantMetric := "general", "general"
			if specific {
				wantTrace, wantMetric = "trace", "metric"
			}
			if received[tracePath] != wantTrace || received[metricPath] != wantMetric {
				t.Fatal("exports not flushed", received)
			}
		})
	}
}

func TestResponseController(t *testing.T) {
	providers(t)
	rec := httptest.NewRecorder()
	Middleware("test")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Error(err)
		}
		w.WriteHeader(500)
	})).ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if !rec.Flushed || rec.Code != 200 {
		t.Fatal(rec)
	}
}

func TestServerFailureAndUnmatchedRoute(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(map[bool]string{false: "status", true: "panic"}[panics], func(t *testing.T) {
			exp, reader := providers(t)
			h := Middleware("test")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if panics {
					panic("internal detail")
				}
				w.WriteHeader(503)
			}))
			func() {
				defer func() {
					if v := recover(); v != nil && !panics {
						t.Fatal(v)
					}
				}()
				h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("CUSTOM", "/private", nil))
			}()
			spans := exp.GetSpans()
			if len(spans) != 1 || spans[0].Status.Code != codes.Error {
				t.Fatal(spans)
			}
			assertAttr(t, spans[0].Attributes, "http.route", "")
			var data metricdata.ResourceMetrics
			if err := reader.Collect(context.Background(), &data); err != nil {
				t.Fatal(err)
			}
			for _, scope := range data.ScopeMetrics {
				for _, m := range scope.Metrics {
					if d, ok := m.Data.(metricdata.Sum[int64]); ok {
						assertAttr(t, d.DataPoints[0].Attributes.ToSlice(), "http.request.method", "_OTHER")
						assertAttr(t, d.DataPoints[0].Attributes.ToSlice(), "http.response.status_class", "5xx")
					}
				}
			}
		})
	}
}

func TestSamplingAndSignalIsolation(t *testing.T) {
	for _, ratio := range []float64{0, 1} {
		t.Run(map[float64]string{0: "zero", 1: "one"}[ratio], func(t *testing.T) {
			isolate(t)
			collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Header().Set("Content-Type", "application/x-protobuf") }))
			defer collector.Close()
			t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", collector.URL+"/v1/traces")
			stop, err := Setup(context.Background(), Options{ServiceName: "test", SampleRatio: ratio})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := stop(context.Background()); err != nil {
					t.Error(err)
				}
			}()
			if _, ok := otel.GetMeterProvider().(*sdkmetric.MeterProvider); ok {
				t.Fatal("metrics unexpectedly enabled")
			}
			ctx, span := otel.Tracer("test").Start(context.Background(), "root")
			if span.IsRecording() != (ratio == 1) {
				t.Fatal("incorrect sampling")
			}
			if TraceIDFromContext(ctx) != span.SpanContext().TraceID().String() || SpanIDFromContext(ctx) != span.SpanContext().SpanID().String() {
				t.Fatal("incorrect identifiers")
			}
			span.End()
		})
	}
}

func BenchmarkMiddleware(b *testing.B) {
	h := Middleware("benchmark")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	req := httptest.NewRequest("GET", "/", nil)
	for b.Loop() {
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
}
