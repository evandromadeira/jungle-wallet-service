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

// TestReversals checks integral reversals, shared uniqueness and atomic failure handling.
func TestReversals(t *testing.T) {
	store, other := repositoryStores(t, "000002_wager_transactions.up.sql", "000003_wallet_ledger_entries.up.sql", "000004_outbox_events.up.sql")
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	initial, _ := domain.NewMoney(10000, "BRL")
	wallet, e := openingService(t, store, "rev-open").Execute(ctx, application.OpenWalletCommand{PlayerID: "p", InitialBalance: initial, CorrelationID: "c"})
	if e != nil {
		t.Fatal(e)
	}
	ws, _ := wallet.Snapshot()
	var n atomic.Int64
	makeService := func(work application.UnitOfWork) *application.ProcessWager {
		s, e := application.NewProcessWager(work, func() (string, error) { return fmt.Sprintf("rev-%d", n.Add(1)), nil }, func() time.Time { return time.Date(2026, 10, 15, 10, 0, 0, 0, time.UTC) })
		if e != nil {
			t.Fatal(e)
		}
		return s
	}
	first, second := makeService(store), makeService(other)
	command := func(id string, kind domain.WagerKind, minor int64, ref string) application.WagerCommand {
		money, _ := domain.NewMoney(minor, "BRL")
		return application.WagerCommand{AuthorizedProviderID: "a", CorrelationID: "c", Request: domain.WagerRequest{ProviderID: "a", ExternalTransactionID: id, IdempotencyKey: id, WalletID: ws.ID, PlayerID: "p", RoundID: "r", GameID: "g", Kind: kind, Money: money, ReferenceExternalTransactionID: ref}}
	}
	run := func(id string, kind domain.WagerKind, minor int64, ref string) domain.WagerState {
		t.Helper()
		got, e := first.Execute(ctx, command(id, kind, minor, ref))
		if e != nil {
			t.Fatal(e)
		}
		s, e := got.Transaction.Snapshot()
		if e != nil {
			t.Fatal(e)
		}
		return s
	}
	assertOutcome := func(s domain.WagerState, status domain.WagerStatus, code domain.FailureCode) {
		t.Helper()
		if s.Status != status || s.FailureCode != code {
			t.Fatalf("%s: %s/%s", s.Request.ExternalTransactionID, s.Status, s.FailureCode)
		}
	}
	assertOutcome(run("bet", domain.WagerBet, 3000, ""), domain.WagerProcessed, "")
	refund := run("refund", domain.WagerRefund, 3000, "bet")
	assertOutcome(refund, domain.WagerProcessed, "")
	duplicate := run("double-refund", domain.WagerRefund, 3000, "bet")
	assertOutcome(duplicate, domain.WagerRejected, domain.FailureAlreadyReversed)
	assertOutcome(run("mixed", domain.WagerRollback, 3000, "bet"), domain.WagerRejected, domain.FailureAlreadyReversed)
	rollbackRefund := run("rollback-refund", domain.WagerRollback, 3000, "refund")
	assertOutcome(rollbackRefund, domain.WagerProcessed, "")
	assertOutcome(run("reopen-bet", domain.WagerRefund, 3000, "bet"), domain.WagerRejected, domain.FailureAlreadyReversed)
	assertOutcome(run("bet2", domain.WagerBet, 1000, ""), domain.WagerProcessed, "")
	rollbackBet := run("rollback-bet", domain.WagerRollback, 1000, "bet2")
	assertOutcome(rollbackBet, domain.WagerProcessed, "")
	assertOutcome(run("win", domain.WagerWin, 2000, ""), domain.WagerProcessed, "")
	rollbackWin := run("rollback-win", domain.WagerRollback, 2000, "win")
	assertOutcome(rollbackWin, domain.WagerProcessed, "")
	assertOutcome(run("partial", domain.WagerRefund, 999, "bet2"), domain.WagerRejected, domain.FailureReferenceMismatch)
	assertOutcome(run("invalid-kind", domain.WagerRefund, 2000, "win"), domain.WagerRejected, domain.FailureReferenceMismatch)
	assertOutcome(run("missing", domain.WagerRefund, 100, "unknown"), domain.WagerPendingReference, "")
	// Concurrent mixed reversals of one fresh debit: exactly one succeeds.
	assertOutcome(run("race-bet", domain.WagerBet, 1000, ""), domain.WagerProcessed, "")
	type outcome struct {
		result application.WagerResult
		err    error
	}
	results := make(chan outcome, 2)
	start := make(chan struct{})
	for i, s := range []*application.ProcessWager{first, second} {
		kind := domain.WagerRefund
		if i == 1 {
			kind = domain.WagerRollback
		}
		c := command(fmt.Sprintf("race-%d", i), kind, 1000, "race-bet")
		go func() { <-start; r, e := s.Execute(ctx, c); results <- outcome{r, e} }()
	}
	close(start)
	success, rejected := 0, 0
	for i := 0; i < 2; i++ {
		select {
		case got := <-results:
			if got.err != nil {
				t.Fatal(got.err)
			}
			s, _ := got.result.Transaction.Snapshot()
			if s.Status == domain.WagerProcessed {
				success++
			} else if s.FailureCode == domain.FailureAlreadyReversed {
				rejected++
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if success != 1 || rejected != 1 {
		t.Fatalf("reversal race: %d/%d", success, rejected)
	}
	// Spend the credited win so its reversal would make the wallet negative.
	assertOutcome(run("large-win", domain.WagerWin, 10000, ""), domain.WagerProcessed, "")
	assertOutcome(run("spend", domain.WagerBet, 16000, ""), domain.WagerProcessed, "")
	insufficient := run("no-funds", domain.WagerRollback, 10000, "large-win")
	assertOutcome(insufficient, domain.WagerRejected, domain.FailureReversalInsufficientFunds)
	replay, e := second.Execute(ctx, command("refund", domain.WagerRefund, 3000, "bet"))
	if e != nil {
		t.Fatal(e)
	}
	replayed, _ := replay.Transaction.Snapshot()
	if !replay.IdempotentReplay || replayed.ResultBalance != refund.ResultBalance {
		t.Fatal("reversal replay changed original result")
	}
	// New keys may retry an unsuccessful reversal after funds arrive; terminal requests remain unchanged.
	assertOutcome(run("fund", domain.WagerWin, 10000, ""), domain.WagerProcessed, "")
	assertOutcome(run("retry-new-operation", domain.WagerRollback, 10000, "large-win"), domain.WagerProcessed, "")
	// A failure in the final event must not consume the original's reversal eligibility.
	assertOutcome(run("atomic-bet", domain.WagerBet, 500, ""), domain.WagerProcessed, "")
	ids := []string{"failed-reversal", "failed-reversal-event", "failed-reversal-ledger", "rev-open-5"}
	index := 0
	failing, e := application.NewProcessWager(store, func() (string, error) { id := ids[index]; index++; return id, nil }, func() time.Time { return time.Date(2026, 10, 15, 11, 0, 0, 0, time.UTC) })
	if e != nil {
		t.Fatal(e)
	}
	if got, e := failing.Execute(ctx, command("atomic-refund", domain.WagerRefund, 500, "atomic-bet")); !errors.Is(e, application.ErrOutboxAlreadyExists) || got != (application.WagerResult{}) {
		t.Fatalf("expected rollback: %v", e)
	}
	assertOutcome(run("atomic-refund", domain.WagerRefund, 500, "atomic-bet"), domain.WagerProcessed, "")
	e = other.WithinTransaction(ctx, func(r application.Repositories) error {
		for _, item := range []struct {
			s         domain.WagerState
			direction domain.LedgerDirection
		}{{refund, domain.LedgerCredit}, {rollbackRefund, domain.LedgerDebit}, {rollbackBet, domain.LedgerCredit}, {rollbackWin, domain.LedgerDebit}} {
			entry, e := r.Ledger.GetByTransaction(ctx, ws.ID, item.s.ID)
			if e != nil {
				return e
			}
			ls, _ := entry.Snapshot()
			if ls.Direction != item.direction || ls.Money != item.s.Request.Money {
				t.Error("wrong reversal ledger")
			}
		}
		for _, s := range []domain.WagerState{duplicate, insufficient} {
			if _, e := r.Ledger.GetByTransaction(ctx, ws.ID, s.ID); !errors.Is(e, application.ErrLedgerNotFound) {
				t.Error("rejection created ledger")
			}
		}
		if _, e := r.Wagers.Get(ctx, "failed-reversal"); !errors.Is(e, application.ErrWagerNotFound) {
			t.Error("failed reversal survived")
		}
		if _, e := r.Outbox.Get(ctx, "failed-reversal-event"); !errors.Is(e, application.ErrOutboxNotFound) {
			t.Error("failed event survived")
		}
		w, e := r.Wallets.Get(ctx, ws.ID)
		if e != nil {
			return e
		}
		s, _ := w.Snapshot()
		minor, _ := s.Balance.Minor()
		if minor != 1000 {
			t.Errorf("unexpected balance %d", minor)
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
}
