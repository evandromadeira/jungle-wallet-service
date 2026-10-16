//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

// TestReferenceWorker verifies durable backoff, recovery by another worker and expiration.
func TestReferenceWorker(t *testing.T) {
	first, second := repositoryStores(t, "000002_wager_transactions.up.sql", "000003_wallet_ledger_entries.up.sql", "000004_outbox_events.up.sql")
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	var clock atomic.Int64
	clock.Store(time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC).UnixNano())
	now := func() time.Time { return time.Unix(0, clock.Load()).UTC() }
	var sequence atomic.Int64
	generator := func() (string, error) { return fmt.Sprintf("reference-%d", sequence.Add(1)), nil }
	processor, e := application.NewProcessWager(first, generator, now)
	if e != nil {
		t.Fatal(e)
	}
	other, e := application.NewProcessWager(second, generator, now)
	if e != nil {
		t.Fatal(e)
	}
	policy := application.ReferencePolicy{BatchSize: 20, MaxAttempts: 3, InitialDelay: time.Second, MaxDelay: 4 * time.Second, TTL: time.Hour, PollInterval: time.Second}
	worker, e := application.NewReferenceWorker(processor, policy)
	if e != nil {
		t.Fatal(e)
	}
	recovered, e := application.NewReferenceWorker(other, policy)
	if e != nil {
		t.Fatal(e)
	}
	initial, _ := domain.NewMoney(10000, "BRL")
	wallet, e := openingService(t, first, "refs-open").Execute(ctx, application.OpenWalletCommand{PlayerID: "p", InitialBalance: initial, CorrelationID: "c"})
	if e != nil {
		t.Fatal(e)
	}
	ws, _ := wallet.Snapshot()
	submit := func(id string, kind domain.WagerKind, ref string, amount int64) domain.WagerState {
		t.Helper()
		money, _ := domain.NewMoney(amount, "BRL")
		result, e := processor.Execute(ctx, application.WagerCommand{AuthorizedProviderID: "a", CorrelationID: "original-correlation", Request: domain.WagerRequest{ProviderID: "a", ExternalTransactionID: id, IdempotencyKey: id, WalletID: ws.ID, PlayerID: "p", RoundID: "r", GameID: "g", Kind: kind, Money: money, ReferenceExternalTransactionID: ref}})
		if e != nil {
			t.Fatal(e)
		}
		s, e := result.Transaction.Snapshot()
		if e != nil {
			t.Fatal(e)
		}
		return s
	}
	pending := submit("refund-first", domain.WagerRefund, "late-bet", 1000)
	if pending.Status != domain.WagerPendingReference {
		t.Fatal("reference not pending")
	}
	if n, e := worker.RunOnce(ctx); e != nil || n != 1 {
		t.Fatalf("initial retry: %d %v", n, e)
	}
	e = second.WithinTransaction(ctx, func(r application.Repositories) error {
		work, e := r.Wagers.ReferenceWork(ctx, pending.ID)
		if e != nil {
			return e
		}
		if work.Attempts != 1 || !work.NextAttemptAt.Equal(now().Add(time.Second)) || work.CorrelationID != "original-correlation" {
			t.Error("retry not persisted")
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	submit("late-bet", domain.WagerBet, "", 1000)
	if n, e := recovered.RunOnce(ctx); e != nil || n != 0 {
		t.Fatalf("backoff ignored: %d %v", n, e)
	}
	clock.Add(int64(time.Second))
	results := make(chan error, 2)
	for _, w := range []*application.ReferenceWorker{worker, recovered} {
		go func() { _, e := w.RunOnce(ctx); results <- e }()
	}
	for i := 0; i < 2; i++ {
		if e := <-results; e != nil {
			t.Fatal(e)
		}
	}
	e = second.WithinTransaction(ctx, func(r application.Repositories) error {
		tx, e := r.Wagers.Get(ctx, pending.ID)
		if e != nil {
			return e
		}
		s, _ := tx.Snapshot()
		if s.Status != domain.WagerProcessed {
			t.Error("pending refund not resolved")
		}
		entry, e := r.Ledger.GetByTransaction(ctx, ws.ID, pending.ID)
		if e != nil {
			return e
		}
		ls, _ := entry.Snapshot()
		if ls.Direction != domain.LedgerCredit {
			t.Error("wrong recovery direction")
		}
		wallet, e := r.Wallets.Get(ctx, ws.ID)
		if e != nil {
			return e
		}
		state, _ := wallet.Snapshot()
		if state.Balance != initial || state.Version != 3 {
			t.Error("duplicate recovered movement")
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	// Exhaustion retries at 1s then 2s; the third unsuccessful attempt rejects durably.
	exhausted := submit("exhaust", domain.WagerWin, "never", 100)
	for i := 0; i < 3; i++ {
		if n, e := recovered.RunOnce(ctx); e != nil || n != 1 {
			t.Fatalf("attempt %d: %d %v", i, n, e)
		}
		if i < 2 {
			clock.Add(int64(time.Second * time.Duration(1<<i)))
		}
	}
	// TTL expiration is independent of the retry counter.
	expired := submit("expire", domain.WagerRollback, "never-either", 100)
	clock.Add(int64(time.Hour))
	if n, e := worker.RunOnce(ctx); e != nil || n != 1 {
		t.Fatalf("TTL: %d %v", n, e)
	}
	e = second.WithinTransaction(ctx, func(r application.Repositories) error {
		for _, id := range []string{exhausted.ID, expired.ID} {
			tx, e := r.Wagers.Get(ctx, id)
			if e != nil {
				return e
			}
			s, _ := tx.Snapshot()
			if s.Status != domain.WagerRejected || s.FailureCode != domain.FailureReferenceNotFound {
				t.Error("expiration not rejected")
			}
			if _, e = r.Ledger.GetByTransaction(ctx, ws.ID, id); !errors.Is(e, application.ErrLedgerNotFound) {
				t.Error("expiration changed funds")
			}
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	// A failed recovery transaction must leave both scheduling and financial state retryable.
	retryable := submit("retryable", domain.WagerRefund, "retry-bet", 500)
	submit("retry-bet", domain.WagerBet, "", 500)
	badProcessor, e := application.NewProcessWager(first, func() (string, error) { return "refs-open-5", nil }, now)
	if e != nil {
		t.Fatal(e)
	}
	badWorker, e := application.NewReferenceWorker(badProcessor, policy)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = badWorker.RunOnce(ctx); !errors.Is(e, application.ErrOutboxAlreadyExists) {
		t.Fatalf("expected injected event failure: %v", e)
	}
	e = second.WithinTransaction(ctx, func(r application.Repositories) error {
		work, e := r.Wagers.ReferenceWork(ctx, retryable.ID)
		if e != nil {
			return e
		}
		if work.Attempts != 0 {
			t.Error("failed attempt was committed")
		}
		if _, e = r.Ledger.GetByTransaction(ctx, ws.ID, retryable.ID); !errors.Is(e, application.ErrLedgerNotFound) {
			t.Error("failed worker left ledger")
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	if n, e := recovered.RunOnce(ctx); e != nil || n != 1 {
		t.Fatalf("recovery after failure: %d %v", n, e)
	}
	// A reference completed before expiry must still resolve after a long worker outage.
	available := submit("available-after-outage", domain.WagerRefund, "outage-bet", 500)
	submit("outage-bet", domain.WagerBet, "", 500)
	clock.Add(int64(2 * time.Hour))
	if n, err := recovered.RunOnce(ctx); err != nil || n != 1 {
		t.Fatalf("recovery beyond TTL: %d %v", n, err)
	}
	if err := second.WithinTransaction(ctx, func(r application.Repositories) error {
		transaction, err := r.Wagers.Get(ctx, available.ID)
		if err != nil {
			return err
		}
		state, err := transaction.Snapshot()
		if err != nil {
			return err
		}
		if state.Status != domain.WagerProcessed {
			t.Fatalf("available reference rejected after outage: %s", state.Status)
		}
		entry, err := r.Ledger.GetByTransaction(ctx, ws.ID, available.ID)
		if err != nil {
			return err
		}
		ledger, err := entry.Snapshot()
		if err != nil {
			return err
		}
		if ledger.Direction != domain.LedgerCredit {
			t.Fatal("missing recovered refund credit")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if n, err := recovered.RunOnce(ctx); err != nil || n != 0 {
		t.Fatalf("terminal refund retried: %d %v", n, err)
	}
	cancelled, stop := context.WithCancel(ctx)
	stop()
	if e = worker.Run(cancelled); !errors.Is(e, context.Canceled) {
		t.Fatalf("worker cancellation: %v", e)
	}
}
