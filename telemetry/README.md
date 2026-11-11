# telemetry

This nested module keeps OpenTelemetry dependencies separate from plinth's
standard-library-only root module. Import `github.com/tcpsystems/plinth/telemetry`.

Call `Setup(ctx, Options{ServiceName: "example", SampleRatio: 1})` at startup.
The service version defaults to `buildinfo.Get().ModuleVersion`. Sampling ranges
from zero (no root spans) to one (all root spans); children follow their parent.
Call the returned shutdown function with a fresh, bounded context before exit.

Export is off until `OTEL_EXPORTER_OTLP_ENDPOINT` or a signal-specific endpoint
is set. A general endpoint such as `http://127.0.0.1:4318` enables traces and
metrics, appending `/v1/traces` and `/v1/metrics`. Signal-specific endpoints
(`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`)
are full URLs and enable only their respective signal when no general endpoint
exists. Standard OTLP headers, TLS, compression and timeout variables are passed
through to the SDK, including signal-specific header overrides.

Wrap a ServeMux once with `Middleware("example")`, after request ID middleware
and before recovery and request logging. It works with `mw.Chain`, `httpx` JSON
handlers, and `logx.Middleware`. Server spans extract W3C trace context and record
the matched `Request.Pattern` after routing. Other routers must set that field
to their template; missing templates use an empty route, never the raw path.

Metrics are `http.server.request.count` and `http.server.request.duration`
(seconds), with `http.route`, `http.request.method` and
`http.response.status_class` (`2xx`, etc.) attributes. Unknown methods use
`_OTHER` to bound cardinality. Streaming, response control and hijacking pass
through the response writer.

Use `http.Client{Transport: ClientTransport("example", nil)}` for outgoing
spans and W3C header injection. Client spans end when the response body reaches
EOF or closes; always close response bodies. Requests and their headers are
cloned before injection.

`Logger(base)` wraps a slog logger, adding `trace_id` and `span_id` to records
written with context-aware methods such as `InfoContext`. Attributes and groups
are preserved. Use it with `logx.Middleware` inside the tracing middleware.

See [the executable example](../examples/telemetry). Check this module with
`go vet ./... && go test -race -count=1 ./...` from this directory.
