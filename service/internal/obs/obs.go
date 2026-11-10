// Package obs is graftd's observability: Prometheus metrics, OpenTelemetry traces,
// and slog lines that carry the trace id.
//
// One id ties them together: a request's trace id IS its request_id ("req_" + 32 hex),
// so the id a customer quotes from an error body opens the trace directly, and every
// log line for that request carries the same id.
package obs

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"graft/kernel/pipeline"
)

type Obs struct {
	Registry   *prometheus.Registry
	Tracer     trace.Tracer
	Propagator propagation.TextMapPropagator

	runDuration *prometheus.HistogramVec
	stepRetries prometheus.Counter
	restarts    *prometheus.CounterVec
	httpReqs    *prometheus.CounterVec
	httpLatency *prometheus.HistogramVec

	provider *sdktrace.TracerProvider
}

// New builds metrics and a tracer. queueDepth is read on every scrape. Traces are
// exported over OTLP/HTTP only if OTEL_EXPORTER_OTLP_ENDPOINT is set; either way
// spans (and so trace ids, and so request ids) exist.
func New(ctx context.Context, queueDepth func(context.Context) (int, error)) (*Obs, error) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	o := &Obs{
		Registry:   reg,
		Propagator: propagation.TraceContext{},
		runDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "graft_run_duration_seconds", Help: "Wall time of a run, claim to recorded result, by final status.",
			Buckets: prometheus.ExponentialBuckets(0.05, 2, 14), // 50 ms .. ~7 min
		}, []string{"status"}),
		stepRetries: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "graft_step_retries_total", Help: "Step attempts that failed or timed out and were retried."}),
		restarts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "graft_run_restarts_total",
			Help: "Runs claimed again: why=lease_expired (a worker died) or why=released (graceful shutdown)."},
			[]string{"why"}),
		httpReqs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "graft_http_requests_total", Help: "HTTP requests by route pattern and status code."}, []string{"route", "code"}),
		httpLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "graft_http_request_duration_seconds", Help: "HTTP request latency by route pattern.",
			Buckets: prometheus.DefBuckets}, []string{"route"}),
	}
	reg.MustRegister(o.runDuration, o.stepRetries, o.restarts, o.httpReqs, o.httpLatency,
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{Name: "graft_queue_depth", Help: "Runs waiting to be claimed."},
			func() float64 {
				c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				n, err := queueDepth(c)
				if err != nil {
					return -1
				}
				return float64(n)
			}))

	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", "graftd"))),
	}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		exp, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, err
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	}
	o.provider = sdktrace.NewTracerProvider(opts...)
	o.Tracer = o.provider.Tracer("graftd")
	otel.SetTextMapPropagator(o.Propagator)
	return o, nil
}

// Shutdown flushes any buffered spans.
func (o *Obs) Shutdown(ctx context.Context) error { return o.provider.Shutdown(ctx) }

// MetricsHandler serves /metrics. graftd mounts it on a separate, internal address.
func (o *Obs) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(o.Registry, promhttp.HandlerOpts{})
}

// RequestID is the request id for a span: "req_" + its trace id.
func RequestID(sc trace.SpanContext) string { return "req_" + sc.TraceID().String() }

// HTTP records one finished request.
func (o *Obs) HTTP(route string, code int, d time.Duration) {
	o.httpReqs.WithLabelValues(route, http.StatusText(code)).Inc()
	o.httpLatency.WithLabelValues(route).Observe(d.Seconds())
}

// Claimed, Events and Finished are the worker's hooks (queue.Hooks).
func (o *Obs) Claimed(claims int, was string) {
	switch {
	case claims > 1 && was == "running":
		o.restarts.WithLabelValues("lease_expired").Inc()
	case claims > 1:
		o.restarts.WithLabelValues("released").Inc()
	}
}

func (o *Obs) Events(evs []pipeline.Event) {
	for _, e := range evs {
		if e.Type == "step_retry" {
			o.stepRetries.Inc()
		}
	}
}

func (o *Obs) Finished(status string, d time.Duration) {
	o.runDuration.WithLabelValues(status).Observe(d.Seconds())
}

// LogHandler wraps a slog.Handler so every record logged with a context that holds a
// span gets that span's trace_id: log lines and traces share one id.
func LogHandler(h slog.Handler) slog.Handler { return traceHandler{h} }

type traceHandler struct{ slog.Handler }

func (t traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
		r.AddAttrs(slog.String("trace_id", sc.TraceID().String()))
	}
	return t.Handler.Handle(ctx, r)
}

func (t traceHandler) WithAttrs(a []slog.Attr) slog.Handler {
	return traceHandler{t.Handler.WithAttrs(a)}
}
func (t traceHandler) WithGroup(n string) slog.Handler { return traceHandler{t.Handler.WithGroup(n)} }
