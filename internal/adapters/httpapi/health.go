package httpapi

import (
	"context"
	"net/http"
	"time"
)

// WithHealth adds public probes without changing business-route authorization.
// Readiness checks must honor cancellation; dependency errors are never exposed.
func WithHealth(business http.Handler, ready func(context.Context) error) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", business)
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, r *http.Request) {
		respond(w, 200, map[string]string{"status": "live"})
	})
	mux.HandleFunc("GET /health/ready", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		if ready == nil || ready(ctx) != nil || ctx.Err() != nil {
			reject(w, 503, "not_ready")
			return
		}
		respond(w, 200, map[string]string{"status": "ready"})
	})
	return mux
}
