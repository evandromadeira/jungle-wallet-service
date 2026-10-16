//go:build integration

package integration

import (
	"context"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

type unavailableLedger struct {
	application.LedgerRepository
	err error
}

// GetByTransaction emulates an unavailable dependency without claiming absence.
func (r unavailableLedger) GetByTransaction(context.Context, string, string) (domain.WalletLedgerEntry, error) {
	return domain.WalletLedgerEntry{}, r.err
}

type unavailableLedgerWork struct {
	application.UnitOfWork
	err error
}

// WithinTransaction preserves real rollback while replacing only the failing lookup.
func (w unavailableLedgerWork) WithinTransaction(ctx context.Context, fn func(application.Repositories) error) error {
	return w.UnitOfWork.WithinTransaction(ctx, func(r application.Repositories) error {
		r.Ledger = unavailableLedger{r.Ledger, w.err}
		return fn(r)
	})
}

// TestPermanentLedgerFailure verifies terminal auditing only for a confirmed missing ledger.
func TestPermanentLedgerFailure(t *testing.T) {
	store, _ := repositoryStores(t, "000002_wager_transactions.up.sql", "000003_wallet_ledger_entries.up.sql", "000004_outbox_events.up.sql")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	initial, _ := domain.NewMoney(10000, "BRL")
	wallet, err := openingService(t, store, "permanent").Execute(ctx, application.OpenWalletCommand{PlayerID: "p", InitialBalance: initial, CorrelationID: "open"})
	if err != nil {
		t.Fatal(err)
	}
	ws, _ := wallet.Snapshot()
	amount, _ := domain.NewMoney(1000, "BRL")
	request := domain.WagerRequest{ProviderID: "a", ExternalTransactionID: "broken-bet", IdempotencyKey: "broken-bet", WalletID: ws.ID, PlayerID: "p", RoundID: "r", GameID: "g", Kind: domain.WagerBet, Money: amount}
	// Emulate legacy/imported inconsistent data; normal processing writes these atomically.
	err = store.WithinTransaction(ctx, func(r application.Repositories) error {
		before, e := r.Wallets.GetForUpdate(ctx, ws.ID)
		if e != nil {
			return e
		}
		at := time.Now().UTC()
		after, e := before.Debit(amount, at)
		if e != nil {
			return e
		}
		state, _ := after.Snapshot()
		pending, e := domain.NewWagerTransaction("broken-reference", request, at)
		if e != nil {
			return e
		}
		processed, e := pending.MarkProcessed(state.Balance, at)
		if e != nil {
			return e
		}
		if _, e = r.Wagers.Create(ctx, processed); e != nil {
			return e
		}
		return r.Wallets.Update(ctx, before, after)
	})
	if err != nil {
		t.Fatal(err)
	}
	processor, err := application.NewProcessWager(store, func() (string, error) { return rand.Text(), nil }, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	request.Kind = domain.WagerRollback
	request.ExternalTransactionID = "rollback-broken"
	request.IdempotencyKey = "rollback-broken"
	request.ReferenceExternalTransactionID = "broken-bet"
	command := application.WagerCommand{AuthorizedProviderID: "a", Request: request, CorrelationID: "audit"}
	unavailable := errors.New("ledger connection unavailable")
	transient, err := application.NewProcessWager(unavailableLedgerWork{store, unavailable}, func() (string, error) { return rand.Text(), nil }, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transient.Execute(ctx, command); !errors.Is(err, unavailable) {
		t.Fatalf("transient error became a terminal outcome: %v", err)
	}
	if err := store.WithinTransaction(ctx, func(r application.Repositories) error {
		_, err := r.Wagers.GetByExternalID(ctx, "a", "rollback-broken")
		if !errors.Is(err, application.ErrWagerNotFound) {
			t.Fatal("transient failure persisted a wager")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		result, e := processor.Execute(ctx, command)
		if e != nil {
			t.Fatal(e)
		}
		state, e := result.Transaction.Snapshot()
		if e != nil {
			t.Fatal(e)
		}
		if state.Status != domain.WagerFailed || state.FailureCode != domain.FailureInfrastructure || result.IdempotentReplay != (attempt == 1) {
			t.Fatalf("unexpected permanent result: %+v", state)
		}
		e = store.WithinTransaction(ctx, func(r application.Repositories) error {
			if _, e := r.Ledger.GetByTransaction(ctx, ws.ID, state.ID); !errors.Is(e, application.ErrLedgerNotFound) {
				t.Fatal("FAILED created a ledger entry")
			}
			current, e := r.Wallets.Get(ctx, ws.ID)
			if e != nil {
				return e
			}
			s, _ := current.Snapshot()
			expected, _ := domain.NewMoney(9000, "BRL")
			if s.Balance != expected || s.Version != 2 {
				t.Fatal("FAILED changed balance or version")
			}
			return nil
		})
		if e != nil {
			t.Fatal(e)
		}
	}
}
