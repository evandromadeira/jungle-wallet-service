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

// betService uses unique IDs safely across concurrent calls.
func betService(t *testing.T, work application.UnitOfWork, prefix string) *application.ProcessBet {
	t.Helper()
	var n atomic.Int64
	service, err := application.NewProcessBet(work, func() (string, error) { return fmt.Sprintf("%s-%d", prefix, n.Add(1)), nil }, func() time.Time { return time.Date(2026, 10, 14, 13, 0, 0, 0, time.UTC) })
	if err != nil {
		t.Fatal(err)
	}
	return service
}

// TestProcessBet checks atomic debit, durable rejections, replay and payload conflicts.
func TestProcessBet(t *testing.T) {
	store, other := repositoryStores(t, "000002_wager_transactions.up.sql", "000003_wallet_ledger_entries.up.sql", "000004_outbox_events.up.sql")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	amount, _ := domain.NewMoney(10000, "BRL")
	wallet, err := openingService(t, store, "open").Execute(ctx, application.OpenWalletCommand{PlayerID: "p", InitialBalance: amount, CorrelationID: "c"})
	if err != nil {
		t.Fatal(err)
	}
	ws, _ := wallet.Snapshot()
	betAmount, _ := domain.NewMoney(8000, "BRL")
	command := application.BetCommand{AuthorizedProviderID: "a", CorrelationID: "c", Request: domain.WagerRequest{ProviderID: "a", ExternalTransactionID: "ext", IdempotencyKey: "key", WalletID: ws.ID, PlayerID: "p", RoundID: "r", GameID: "g", Kind: domain.WagerBet, Money: betAmount}}
	service := betService(t, store, "bet")
	result, err := service.Execute(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	original, _ := result.Transaction.Snapshot()
	if original.Status != domain.WagerProcessed || result.IdempotentReplay {
		t.Fatal("bet not processed")
	}
	rejectedCommand := command
	rejectedCommand.Request.ExternalTransactionID = "rejected"
	rejectedCommand.Request.IdempotencyKey = "rejected"
	rejected, err := service.Execute(ctx, rejectedCommand)
	if err != nil {
		t.Fatal(err)
	}
	rs, _ := rejected.Transaction.Snapshot()
	if rs.Status != domain.WagerRejected || rs.FailureCode != domain.FailureInsufficientFunds {
		t.Fatal("insufficient funds not audited")
	}
	err = other.WithinTransaction(ctx, func(r application.Repositories) error {
		entry, e := r.Ledger.GetByTransaction(ctx, ws.ID, original.ID)
		if e != nil {
			return e
		}
		ls, _ := entry.Snapshot()
		minor, _ := ls.BalanceAfter.Minor()
		if minor != 2000 {
			t.Error("wrong debit")
		}
		if _, e = r.Ledger.GetByTransaction(ctx, ws.ID, rs.ID); !errors.Is(e, application.ErrLedgerNotFound) {
			t.Error("rejection created ledger")
		}
		before, e := r.Wallets.GetForUpdate(ctx, ws.ID)
		if e != nil {
			return e
		}
		after, e := before.Credit(amount, time.Date(2026, 10, 14, 14, 0, 0, 0, time.UTC))
		if e != nil {
			return e
		}
		return r.Wallets.Update(ctx, before, after)
	})
	if err != nil {
		t.Fatal(err)
	}
	// A new service/pool must replay both success and rejection despite a later balance change.
	replayService := betService(t, other, "replay")
	for _, c := range []application.BetCommand{command, rejectedCommand} {
		got, e := replayService.Execute(ctx, c)
		if e != nil {
			t.Fatal(e)
		}
		s, _ := got.Transaction.Snapshot()
		minor, _ := s.ResultBalance.Minor()
		if !got.IdempotentReplay || minor != 2000 {
			t.Fatal("replay did not preserve result")
		}
	}
	alias := command
	alias.Request.IdempotencyKey = "alternative"
	if got, e := replayService.Execute(ctx, alias); e != nil || !got.IdempotentReplay {
		t.Fatalf("alternative key replay: %v", e)
	}
	conflict := command
	conflict.Request.GameID = "changed"
	if _, e := service.Execute(ctx, conflict); !errors.Is(e, domain.ErrWagerPayloadConflict) {
		t.Fatalf("changed payload: %v", e)
	}
	conflict = command
	conflict.Request.IdempotencyKey = "rejected"
	if _, e := service.Execute(ctx, conflict); !errors.Is(e, domain.ErrWagerPayloadConflict) {
		t.Fatalf("crossed identities: %v", e)
	}
	unauthorized := command
	unauthorized.AuthorizedProviderID = "b"
	if _, e := service.Execute(ctx, unauthorized); !errors.Is(e, application.ErrProviderMismatch) {
		t.Fatal(e)
	}
	// Fail the last event write by reusing an already persisted opening event ID.
	ids := []string{"failed-tx", "failed-event", "failed-entry", "open-5"}
	i := 0
	failing, e := application.NewProcessBet(store, func() (string, error) { id := ids[i]; i++; return id, nil }, func() time.Time { return time.Date(2026, 10, 14, 15, 0, 0, 0, time.UTC) })
	if e != nil {
		t.Fatal(e)
	}
	fresh := command
	fresh.Request.ExternalTransactionID = "failed"
	fresh.Request.IdempotencyKey = "failed"
	if got, e := failing.Execute(ctx, fresh); !errors.Is(e, application.ErrOutboxAlreadyExists) || got != (application.BetResult{}) {
		t.Fatalf("failure leaked result: %v", e)
	}
	err = other.WithinTransaction(ctx, func(r application.Repositories) error {
		if _, e := r.Wagers.Get(ctx, "failed-tx"); !errors.Is(e, application.ErrWagerNotFound) {
			t.Error("failed bet persisted")
		}
		if _, e := r.Outbox.Get(ctx, "failed-event"); !errors.Is(e, application.ErrOutboxNotFound) {
			t.Error("failed event persisted")
		}
		if _, e := r.Ledger.GetByTransaction(ctx, ws.ID, "failed-tx"); !errors.Is(e, application.ErrLedgerNotFound) {
			t.Error("failed ledger persisted")
		}
		got, e := r.Wallets.Get(ctx, ws.ID)
		if e != nil {
			return e
		}
		s, _ := got.Snapshot()
		minor, _ := s.Balance.Minor()
		if minor != 12000 || s.Version != 3 {
			t.Error("failed debit persisted")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestConcurrentBets uses independent pools in one process, not the required three processes.
func TestConcurrentBets(t *testing.T) {
	first, second := repositoryStores(t, "000002_wager_transactions.up.sql", "000003_wallet_ledger_entries.up.sql", "000004_outbox_events.up.sql")
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	services := []*application.ProcessBet{betService(t, first, "a"), betService(t, second, "b")}
	for _, duplicates := range []bool{false, true} {
		name := "distinct"
		count := 2
		if duplicates {
			name = "duplicates"
			count = 50
		}
		amount, _ := domain.NewMoney(10000, "BRL")
		wallet, e := openingService(t, first, name).Execute(ctx, application.OpenWalletCommand{PlayerID: name, InitialBalance: amount, CorrelationID: "c"})
		if e != nil {
			t.Fatal(e)
		}
		ws, _ := wallet.Snapshot()
		stake, _ := domain.NewMoney(8000, "BRL")
		type outcome struct {
			result application.BetResult
			err    error
		}
		results := make(chan outcome, count)
		start := make(chan struct{})
		for i := 0; i < count; i++ {
			id := fmt.Sprintf("%s-%d", name, i)
			if duplicates {
				id = name
			}
			command := application.BetCommand{AuthorizedProviderID: "provider", CorrelationID: "c", Request: domain.WagerRequest{ProviderID: "provider", ExternalTransactionID: id, IdempotencyKey: id, WalletID: ws.ID, PlayerID: name, RoundID: "r", GameID: "g", Kind: domain.WagerBet, Money: stake}}
			service := services[i%2]
			go func() { <-start; r, e := service.Execute(ctx, command); results <- outcome{r, e} }()
		}
		close(start)
		processed, rejected, replays := 0, 0, 0
		transactionIDs := map[string]domain.WagerStatus{}
		for i := 0; i < count; i++ {
			select {
			case got := <-results:
				if got.err != nil {
					t.Fatal(got.err)
				}
				s, _ := got.result.Transaction.Snapshot()
				transactionIDs[s.ID] = s.Status
				if got.result.IdempotentReplay {
					replays++
				} else if s.Status == domain.WagerProcessed {
					processed++
				} else if s.Status == domain.WagerRejected {
					rejected++
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
		}
		if processed != 1 || (!duplicates && (rejected != 1 || replays != 0)) || (duplicates && (replays != 49 || rejected != 0)) {
			t.Fatalf("processed=%d rejected=%d replays=%d", processed, rejected, replays)
		}
		e = second.WithinTransaction(ctx, func(r application.Repositories) error {
			got, e := r.Wallets.Get(ctx, ws.ID)
			if e != nil {
				return e
			}
			s, _ := got.Snapshot()
			minor, _ := s.Balance.Minor()
			if minor != 2000 || s.Version != 2 {
				t.Error("duplicate debit")
			}
			for id, status := range transactionIDs {
				_, e := r.Ledger.GetByTransaction(ctx, ws.ID, id)
				if status == domain.WagerProcessed && e != nil {
					return e
				}
				if status == domain.WagerRejected && !errors.Is(e, application.ErrLedgerNotFound) {
					t.Error("rejection ledger")
				}
			}
			return nil
		})
		if e != nil {
			t.Fatal(e)
		}
	}
}
