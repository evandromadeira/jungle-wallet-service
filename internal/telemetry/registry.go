// Package telemetry provides bounded-cardinality process metrics and safe JSON operation logs.
package telemetry

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"
)

type key struct{}
type sample struct {
	count   uint64
	seconds float64
}

// Registry owns counters for one process. IDs appear only in logs, never metric labels.
type Registry struct {
	Collect    func(context.Context) (map[string]float64, error)
	mu         sync.Mutex
	operations map[string]sample
	counters   map[string]uint64
	log        *slog.Logger
}

// New creates an isolated registry; callers own the logger.
func New(logger *slog.Logger) *Registry {
	return &Registry{operations: make(map[string]sample), counters: make(map[string]uint64), log: logger}
}

// With carries a shared registry through HTTP requests and worker contexts.
func With(ctx context.Context, r *Registry) context.Context { return context.WithValue(ctx, key{}, r) }

// From returns nil when instrumentation was not configured.
func From(ctx context.Context) *Registry { r, _ := ctx.Value(key{}).(*Registry); return r }

// Count increments a fixed-name counter; callers must never use request values as names.
func (r *Registry) Count(name string) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counters[name]++
}

// Observe records completion latency and result; operation and status are internal enums.
func (r *Registry) Observe(ctx context.Context, operation, status string, start time.Time, attrs ...any) {
	if r == nil {
		return
	}
	elapsed := time.Since(start).Seconds()
	k := fmt.Sprintf("operation=%q,status=%q", operation, status)
	r.mu.Lock()
	s := r.operations[k]
	s.count++
	s.seconds += elapsed
	r.operations[k] = s
	r.mu.Unlock()
	if r.log != nil {
		fields := []any{"operation", operation, "status", status, "durationSeconds", elapsed}
		fields = append(fields, attrs...)
		r.log.InfoContext(ctx, "operation completed", fields...)
	}
}

// Write emits Prometheus counters and summaries with stable ordering.
func (r *Registry) Write(w io.Writer) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	fmt.Fprintln(w, "# TYPE wallet_operations_total counter")
	fmt.Fprintln(w, "# TYPE wallet_processing_duration_seconds summary")
	keys := make([]string, 0, len(r.operations))
	for k := range r.operations {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		s := r.operations[k]
		fmt.Fprintf(w, "wallet_operations_total{%s} %d\nwallet_processing_duration_seconds_count{%s} %d\nwallet_processing_duration_seconds_sum{%s} %g\n", k, s.count, k, s.count, k, s.seconds)
	}
	names := []string{"wallet_duplicates_total", "wallet_concurrency_conflicts_total", "wallet_idempotency_conflicts_total", "wallet_sqs_retries_total", "wallet_outbox_retries_total", "wallet_reference_retries_total"}
	for _, name := range names {
		fmt.Fprintf(w, "# TYPE %s counter\n%s %d\n", name, name, r.counters[name])
	}
}

// Gather refreshes dependency gauges; callers must bound the context.
func (r *Registry) Gather(ctx context.Context) (map[string]float64, error) {
	if r == nil || r.Collect == nil {
		return nil, nil
	}
	return r.Collect(ctx)
}

// WriteGauges emits internally named measurements in stable order.
func WriteGauges(w io.Writer, values map[string]float64) {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Fprintf(w, "# TYPE %s gauge\n%s %g\n", name, name, values[name])
	}
}
