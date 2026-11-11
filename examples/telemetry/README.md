# HTTP telemetry example

From this directory, start with `go run .`, then request
`curl http://127.0.0.1:8080/hello/world`. Stop with Ctrl-C.
Export is disabled by default. To send traces and metrics to a local collector:

```sh
OTEL_EXPORTER_OTLP_ENDPOINT=http://127.0.0.1:4318 go run .
```

Request logs include trace and span IDs when tracing is enabled.
