//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

// TestInboxProcessing verifies durable receipt completion and shared financial atomicity.
func TestInboxProcessing(t *testing.T) {
	store, other := repositoryStores(t, "000002_wager_transactions.up.sql", "000003_wallet_ledger_entries.up.sql", "000004_outbox_events.up.sql", "000005_inbox_messages.up.sql")
	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Second)
	defer cancel()
	amount, _ := domain.NewMoney(10000, "BRL")
	wallet, e := openingService(t, store, "inbox-open").Execute(ctx, application.OpenWalletCommand{PlayerID: "p", InitialBalance: amount, CorrelationID: "c"})
	if e != nil {
		t.Fatal(e)
	}
	ws, _ := wallet.Snapshot()
	var n atomic.Int64
	now := func() time.Time { return time.Date(2026, 10, 15, 15, 0, 0, 0, time.UTC) }
	makeProcessor := func(work application.UnitOfWork) *application.ProcessWager {
		p, e := application.NewProcessWager(work, func() (string, error) { return fmt.Sprintf("inbox-%d", n.Add(1)), nil }, now)
		if e != nil {
			t.Fatal(e)
		}
		return p
	}
	first, second := makeProcessor(store), makeProcessor(other)
	message := func(id, external string, kind domain.WagerKind, minor int64, reference string) []byte {
		m := application.WagerMessage{MessageID: id, Type: "WagerTransactionRequested", OccurredAt: now()}
		d := &m.Data
		d.ProviderID = "a"
		d.ExternalTransactionID = external
		d.IdempotencyKey = external
		d.WalletID = ws.ID
		d.PlayerID = "p"
		d.RoundID = "r"
		d.GameID = "g"
		d.Kind = kind
		d.Money, _ = domain.NewMoney(minor, "BRL")
		d.ReferenceExternalTransactionID = reference
		body, e := json.Marshal(m)
		if e != nil {
			t.Fatal(e)
		}
		return body
	}
	body := message("m1", "bet", domain.WagerBet, 8000, "")
	type outcome struct {
		r application.WagerResult
		e error
	}
	results := make(chan outcome, 2)
	start := make(chan struct{})
	for _, p := range []*application.ProcessWager{first, second} {
		go func() { <-start; r, e := p.HandleMessage(ctx, "consumer", "a", "c", body); results <- outcome{r, e} }()
	}
	close(start)
	replays := 0
	var original domain.WagerState
	for i := 0; i < 2; i++ {
		got := <-results
		if got.e != nil {
			t.Fatal(got.e)
		}
		if got.r.IdempotentReplay {
			replays++
		}
		original, _ = got.r.Transaction.Snapshot()
	}
	if replays != 1 || original.Status != domain.WagerProcessed {
		t.Fatal("duplicate delivery reapplied bet")
	}
	// Exact envelope bytes define message identity, even if whitespace does not change JSON semantics.
	if _, e = first.HandleMessage(ctx, "consumer", "a", "c", append([]byte(" "), body...)); !errors.Is(e, application.ErrInboxPayloadConflict) {
		t.Fatalf("message hash conflict: %v", e)
	}
	if _, e = first.HandleMessage(ctx, "consumer", "b", "c", body); !errors.Is(e, application.ErrProviderMismatch) {
		t.Fatal("unauthorized replay accepted")
	}
	// A different message carrying the same financial operation is a financial replay.
	replay, e := second.HandleMessage(ctx, "consumer", "a", "c", message("m2", "bet", domain.WagerBet, 8000, ""))
	if e != nil || !replay.IdempotentReplay {
		t.Fatalf("financial replay: %v", e)
	}
	if _, e = second.HandleMessage(ctx, "audit", "a", "c", body); e != nil {
		t.Fatal(e)
	}
	rejection, e := first.HandleMessage(ctx, "consumer", "a", "c", message("rejected", "too-big", domain.WagerBet, 8000, ""))
	if e != nil {
		t.Fatal(e)
	}
	rs, _ := rejection.Transaction.Snapshot()
	if rs.Status != domain.WagerRejected {
		t.Fatal("fixture not rejected")
	}
	pending, e := first.HandleMessage(ctx, "consumer", "a", "c", message("pending", "refund", domain.WagerRefund, 100, "missing"))
	if e != nil {
		t.Fatal(e)
	}
	ps, _ := pending.Transaction.Snapshot()
	if ps.Status != domain.WagerPendingReference {
		t.Fatal("fixture not pending")
	}
	// Direct invocation and message handling share business identity checks.
	stake, _ := domain.NewMoney(8000, "BRL")
	direct, e := second.Execute(ctx, application.WagerCommand{AuthorizedProviderID: "a", CorrelationID: "c", Request: domain.WagerRequest{ProviderID: "a", ExternalTransactionID: "bet", IdempotencyKey: "bet", WalletID: ws.ID, PlayerID: "p", RoundID: "r", GameID: "g", Kind: domain.WagerBet, Money: stake}})
	if e != nil || !direct.IdempotentReplay {
		t.Fatalf("cross-entry replay: %v", e)
	}
	// Fail after inbox receipt and financial writes, at the final outbox insert.
	ids := []string{"inbox-failed-tx", "inbox-failed-event", "inbox-failed-ledger", "inbox-open-5"}
	index := 0
	failing, e := application.NewProcessWager(store, func() (string, error) { id := ids[index]; index++; return id, nil }, now)
	if e != nil {
		t.Fatal(e)
	}
	failedBody := message("failed", "failed-bet", domain.WagerBet, 100, "")
	if _, e = failing.HandleMessage(ctx, "consumer", "a", "c", failedBody); !errors.Is(e, application.ErrOutboxAlreadyExists) {
		t.Fatalf("expected event failure: %v", e)
	}
	e = other.WithinTransaction(ctx, func(r application.Repositories) error {
		for _, id := range []string{"m1", "m2", "rejected", "pending"} {
			receipt, e := r.Inbox.GetForUpdate(ctx, "consumer", id)
			if e != nil {
				return e
			}
			if receipt.CompletedAt == nil {
				t.Errorf("message %s not complete", id)
			}
		}
		if _, e := r.Inbox.GetForUpdate(ctx, "consumer", "failed"); !errors.Is(e, application.ErrInboxNotFound) {
			t.Error("receipt survived rollback")
		}
		if _, e := r.Wagers.Get(ctx, "inbox-failed-tx"); !errors.Is(e, application.ErrWagerNotFound) {
			t.Error("operation survived rollback")
		}
		if _, e := r.Ledger.GetByTransaction(ctx, ws.ID, "inbox-failed-tx"); !errors.Is(e, application.ErrLedgerNotFound) {
			t.Error("ledger survived rollback")
		}
		if _, e := r.Outbox.Get(ctx, "inbox-failed-event"); !errors.Is(e, application.ErrOutboxNotFound) {
			t.Error("event survived rollback")
		}
		w, e := r.Wallets.Get(ctx, ws.ID)
		if e != nil {
			return e
		}
		s, _ := w.Snapshot()
		minor, _ := s.Balance.Minor()
		if minor != 2000 || s.Version != 2 {
			t.Error("unexpected balance")
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = second.HandleMessage(ctx, "consumer", "a", "c", failedBody); e != nil {
		t.Fatalf("retry after rollback: %v", e)
	}
	for _, bad := range [][]byte{[]byte(`{}`), []byte(`null`), append(append([]byte{}, body...), []byte(` {}`)...)} {
		if _, e = first.HandleMessage(ctx, "consumer", "a", "c", bad); !errors.Is(e, application.ErrInvalidInboxMessage) {
			t.Fatalf("invalid envelope: %v", e)
		}
	}
}
