package httpapi

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestHealth verifies public probe isolation, dependency failure and bounded cancellation.
func TestHealth(t *testing.T) {
	for _, tc := range []struct {
		name    string
		failure bool
	}{{"healthy", false}, {"dependency down", true}} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			h := WithHealth(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) }), func(ctx context.Context) error {
				calls++
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > 3*time.Second {
					t.Fatal("missing readiness deadline")
				}
				if tc.failure {
					return errors.New("secret database connection")
				}
				return nil
			})
			for _, path := range []string{"/health/live", "/health/ready", "/wallets/x"} {
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
				want := 200
				if path == "/health/ready" && tc.failure {
					want = 503
				}
				if path == "/wallets/x" {
					want = 401
				}
				if rec.Code != want || strings.Contains(rec.Body.String(), "secret") {
					t.Fatal("invalid health response")
				}
			}
			if calls != 1 {
				t.Fatal("liveness or business route checked dependencies")
			}
		})
	}
	h := WithHealth(http.NotFoundHandler(), func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/health/ready", nil).WithContext(ctx))
	if rec.Code != 503 {
		t.Fatal("cancelled probe reported ready")
	}
}
