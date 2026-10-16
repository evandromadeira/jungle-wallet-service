package telemetry

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestConcurrentRegistry checks exact totals and excludes identifiers from metric labels.
func TestConcurrentRegistry(t *testing.T) {
	registry := New(nil)
	ctx := With(context.Background(), registry)
	if From(ctx) != registry {
		t.Fatal("registry context lost")
	}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			registry.Count("wallet_duplicates_total")
			registry.Observe(ctx, "sqs_wager", "PROCESSED", time.Now(), "walletId", "private-wallet")
		}()
	}
	wg.Wait()
	var out bytes.Buffer
	registry.Write(&out)
	if !strings.Contains(out.String(), "wallet_duplicates_total 50") || !strings.Contains(out.String(), `wallet_processing_duration_seconds_count{operation="sqs_wager",status="PROCESSED"} 50`) {
		t.Fatal("counter updates lost")
	}
	if strings.Contains(out.String(), "private-wallet") {
		t.Fatal("identifier leaked into metric")
	}
}
