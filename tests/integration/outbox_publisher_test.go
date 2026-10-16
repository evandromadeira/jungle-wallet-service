//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

type eventSenderFunc func(context.Context, []byte) error

// Send delegates to a controlled transport while persistence remains real PostgreSQL.
func (f eventSenderFunc) Send(c context.Context, b []byte) error { return f(c, b) }

type rollbackDelivery struct{ application.UnitOfWork }

// WithinTransaction injects interruption after sending and before committing confirmation.
func (w rollbackDelivery) WithinTransaction(c context.Context, f func(application.Repositories) error) error {
	return w.UnitOfWork.WithinTransaction(c, func(r application.Repositories) error {
		if e := f(r); e != nil {
			return e
		}
		return errors.New("injected confirmation failure")
	})
}

// TestOutboxPublisher checks durable retry, concurrent claims and uncertain publication.
func TestOutboxPublisher(t *testing.T) {
	for _, scenario := range []string{"retry", "concurrent", "confirmation failure", "timeout"} {
		t.Run(scenario, func(t *testing.T) {
			store, peer := repositoryStores(t, "000002_wager_transactions.up.sql", "000003_wallet_ledger_entries.up.sql", "000004_outbox_events.up.sql")
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			amount, _ := domain.NewMoney(100, "BRL")
			_, err := openingService(t, store, "outbox").Execute(ctx, application.OpenWalletCommand{PlayerID: "player", InitialBalance: amount, CorrelationID: "trace"})
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC().Add(time.Minute)
			policy := application.OutboxPolicy{BatchSize: 1, SendTimeout: time.Second, InitialDelay: time.Minute, MaxDelay: time.Hour, PollInterval: time.Second}
			makePublisher := func(work application.UnitOfWork, sender eventSenderFunc) *application.OutboxPublisher {
				p, e := application.NewOutboxPublisher(work, sender, func() time.Time { return now }, policy)
				if e != nil {
					t.Fatal(e)
				}
				return p
			}
			switch scenario {
			case "retry", "timeout":
				calls := 0
				failed := makePublisher(store, func(c context.Context, b []byte) error {
					calls++
					if scenario == "timeout" {
						<-c.Done()
						return c.Err()
					}
					return errors.New("private payload must not be persisted")
				})
				// Both opening events fail and remain durable but not immediately due.
				for i := 0; i < 2; i++ {
					if n, e := failed.RunOnce(ctx); e != nil || n != 1 {
						t.Fatalf("failure attempt %d %v", n, e)
					}
				}
				if n, e := failed.RunOnce(ctx); e != nil || n != 0 {
					t.Fatalf("premature retry %d %v", n, e)
				}
				now = now.Add(time.Minute)
				err = peer.WithinTransaction(ctx, func(r application.Repositories) error {
					d, e := r.Outbox.ClaimDue(ctx, now)
					if e == nil && d.Attempts != 1 {
						t.Errorf("attempts=%d", d.Attempts)
					}
					return e
				})
				if err != nil {
					t.Fatal(err)
				}
				ok := makePublisher(peer, func(context.Context, []byte) error { return nil })
				for i := 0; i < 2; i++ {
					if n, e := ok.RunOnce(ctx); e != nil || n != 1 {
						t.Fatalf("retry %d %v", n, e)
					}
				}
				if n, e := ok.RunOnce(ctx); e != nil || n != 0 {
					t.Fatalf("published selected %d %v", n, e)
				}
				if calls != 2 {
					t.Fatal(calls)
				}
			case "confirmation failure":
				var bodies []string
				sender := eventSenderFunc(func(_ context.Context, b []byte) error { bodies = append(bodies, string(b)); return nil })
				if n, e := makePublisher(rollbackDelivery{store}, sender).RunOnce(ctx); e == nil || n != 0 {
					t.Fatalf("fault not propagated %d %v", n, e)
				}
				if n, e := makePublisher(peer, sender).RunOnce(ctx); e != nil || n != 1 {
					t.Fatalf("recovery %d %v", n, e)
				}
				if len(bodies) != 2 || bodies[0] != bodies[1] {
					t.Fatal("republication changed immutable envelope")
				}
			case "concurrent":
				entered := make(chan string, 2)
				release := make(chan struct{})
				sender := eventSenderFunc(func(c context.Context, b []byte) error {
					entered <- string(b)
					select {
					case <-release:
						return nil
					case <-c.Done():
						return c.Err()
					}
				})
				var wg sync.WaitGroup
				results := make(chan error, 2)
				for _, w := range []application.UnitOfWork{store, peer} {
					p := makePublisher(w, sender)
					wg.Add(1)
					go func() {
						defer wg.Done()
						n, e := p.RunOnce(ctx)
						if e == nil && n != 1 {
							e = errors.New("expected one claim")
						}
						results <- e
					}()
				}
				var bodies []string
				for i := 0; i < 2; i++ {
					select {
					case b := <-entered:
						bodies = append(bodies, b)
					case <-ctx.Done():
						t.Fatal("workers did not claim independently")
					}
				}
				close(release)
				wg.Wait()
				close(results)
				for e := range results {
					if e != nil {
						t.Fatal(e)
					}
				}
				if bodies[0] == bodies[1] {
					t.Fatal("workers sent the same event")
				}
				if n, e := makePublisher(peer, sender).RunOnce(ctx); e != nil || n != 0 {
					t.Fatalf("events not confirmed %d %v", n, e)
				}
			}
		})
	}
}
