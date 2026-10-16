package application

import (
	"context"
	"testing"
	"time"
)

// TestOutboxDelay verifies exponential growth, a non-power-of-two cap and saturation.
func TestOutboxDelay(t *testing.T) {
	p := OutboxPublisher{policy: OutboxPolicy{InitialDelay: time.Second, MaxDelay: 5 * time.Second}}
	for i, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second} {
		if got := p.delay(i); got != want {
			t.Fatalf("attempt %d: %v != %v", i, got, want)
		}
	}
	if p.delay(2147483647) != 5*time.Second {
		t.Fatal("large retry count did not saturate")
	}
	p.policy.InitialDelay = time.Duration(1 << 62)
	p.policy.MaxDelay = time.Duration(1<<63 - 1)
	if p.delay(1) != p.policy.MaxDelay {
		t.Fatal("duration overflow")
	}
}

type canceledOutboxWork struct{}

// WithinTransaction models the transaction boundary rejecting cancellation.
func (canceledOutboxWork) WithinTransaction(c context.Context, _ func(Repositories) error) error {
	return c.Err()
}

// TestOutboxCancellation ensures shutdown propagates without attempting a send.
func TestOutboxCancellation(t *testing.T) {
	p := OutboxPublisher{work: canceledOutboxWork{}, policy: OutboxPolicy{BatchSize: 1}}
	c, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.Run(c); err != context.Canceled {
		t.Fatal(err)
	}
}
