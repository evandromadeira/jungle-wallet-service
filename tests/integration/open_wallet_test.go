//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

// openingService supplies predictable identities without coupling production code to tests.
func openingService(t *testing.T, work application.UnitOfWork, prefix string, ids ...string) *application.OpenWallet {
	t.Helper()
	index := 0
	generator := func() (string, error) {
		index++
		if len(ids) > 0 {
			if index > len(ids) {
				return "", errors.New("unexpected ID request")
			}
			return ids[index-1], nil
		}
		return fmt.Sprintf("%s-%d", prefix, index), nil
	}
	service, err := application.NewOpenWallet(work, generator, func() time.Time { return time.Date(2026, 10, 14, 12, 0, 0, 123456789, time.UTC) })
	if err != nil {
		t.Fatal(err)
	}
	return service
}

// TestOpenWallet executes the actual use case against all four PostgreSQL adapters.
func TestOpenWallet(t *testing.T) {
	store, reader := repositoryStores(t, "000002_wager_transactions.up.sql", "000003_wallet_ledger_entries.up.sql", "000004_outbox_events.up.sql")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	amount, _ := domain.NewMoney(10000, "BRL")
	command := application.OpenWalletCommand{PlayerID: "player", InitialBalance: amount, CorrelationID: "correlation"}
	service := openingService(t, store, "positive")
	wallet, err := service.Execute(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	state, _ := wallet.Snapshot()
	if state.ID != "positive-1" || state.Version != 1 || state.Balance != amount || state.CreatedAt.Nanosecond() != 123456000 {
		t.Fatal("invalid opening result")
	}
	err = reader.WithinTransaction(ctx, func(r application.Repositories) error {
		persisted, e := r.Wallets.Get(ctx, state.ID)
		if e != nil {
			return e
		}
		ps, _ := persisted.Snapshot()
		if ps != state {
			t.Error("returned state differs from committed wallet")
		}
		tx, e := r.Wagers.Get(ctx, "positive-2")
		if e != nil {
			return e
		}
		ts, _ := tx.Snapshot()
		if ts.Status != domain.WagerProcessed || ts.Origin != domain.WagerInternal || ts.ResultBalance != amount {
			t.Error("invalid opening transaction")
		}
		ledger, e := r.Ledger.GetByTransaction(ctx, state.ID, "positive-2")
		if e != nil {
			return e
		}
		ls, _ := ledger.Snapshot()
		zero, _ := domain.ZeroMoney("BRL")
		if ls.BalanceBefore != zero || ls.BalanceAfter != amount {
			t.Error("invalid opening ledger")
		}
		for id, kind := range map[string]string{"positive-4": "WagerTransactionProcessed", "positive-5": "WalletBalanceChanged"} {
			event, e := r.Outbox.Get(ctx, id)
			if e != nil {
				return e
			}
			if event.EventType != kind || event.CorrelationID != "correlation" {
				t.Error("invalid opening event")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	zero, _ := domain.ZeroMoney("BRL")
	zeroWallet, err := openingService(t, store, "", "zero-wallet").Execute(ctx, application.OpenWalletCommand{PlayerID: "zero-player", InitialBalance: zero, CorrelationID: "zero-correlation"})
	if err != nil {
		t.Fatal(err)
	}
	zs, _ := zeroWallet.Snapshot()
	if zs.Balance != zero || zs.Version != 1 {
		t.Fatal("zero opening changed balance or version")
	}
	duplicate, err := openingService(t, store, "duplicate").Execute(ctx, command)
	if !errors.Is(err, application.ErrWalletAlreadyExists) || duplicate != (domain.Wallet{}) {
		t.Fatalf("duplicate opening: %v", err)
	}
	// Collision on the final event happens after wallet, transaction, ledger and first event writes.
	command.PlayerID = "failed-player"
	failed, err := openingService(t, store, "", "failed-wallet", "failed-tx", "failed-ledger", "failed-event", "positive-5").Execute(ctx, command)
	if !errors.Is(err, application.ErrOutboxAlreadyExists) || failed != (domain.Wallet{}) {
		t.Fatalf("expected final event failure: %v", err)
	}
	err = reader.WithinTransaction(ctx, func(r application.Repositories) error {
		for _, id := range []string{"duplicate-1", "failed-wallet"} {
			if _, e := r.Wallets.Get(ctx, id); !errors.Is(e, application.ErrWalletNotFound) {
				t.Errorf("wallet survived: %s", id)
			}
		}
		if _, e := r.Wagers.Get(ctx, "failed-tx"); !errors.Is(e, application.ErrWagerNotFound) {
			t.Error("transaction survived")
		}
		if _, e := r.Ledger.GetByTransaction(ctx, "failed-wallet", "failed-tx"); !errors.Is(e, application.ErrLedgerNotFound) {
			t.Error("ledger survived")
		}
		if _, e := r.Outbox.Get(ctx, "failed-event"); !errors.Is(e, application.ErrOutboxNotFound) {
			t.Error("event survived")
		}
		if _, e := r.Outbox.Get(ctx, "positive-5"); e != nil {
			return e
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	cancelled, stop := context.WithCancel(ctx)
	stop()
	if _, err = service.Execute(cancelled, command); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	negative, _ := domain.NewMoney(-1, "BRL")
	command.InitialBalance = negative
	if _, err = service.Execute(ctx, command); !errors.Is(err, domain.ErrNegativeBalance) {
		t.Fatal(err)
	}
}
