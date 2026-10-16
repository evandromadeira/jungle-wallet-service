//go:build integration

package integration

import (
	"context"
	"crypto/rand"
	"fmt"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

// TestConcurrentReconciliation exercises statement snapshots during concurrent committed debits.
func TestConcurrentReconciliation(t *testing.T) {
	store, _, _ := restrictedStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	ids := func() (string, error) { return rand.Text(), nil }
	opening, err := application.NewOpenWallet(store, ids, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	initial, _ := domain.NewMoney(10000, "BRL")
	wallet, err := opening.Execute(ctx, application.OpenWalletCommand{PlayerID: "concurrent-audit", InitialBalance: initial, CorrelationID: "open"})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := wallet.Snapshot()
	processor, err := application.NewProcessWager(store, ids, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	audit, err := application.NewReconcileWallet(store, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	amount, _ := domain.NewMoney(100, "BRL")
	start := make(chan struct{})
	failures := make(chan error, 3)
	var wg sync.WaitGroup
	for worker := 0; worker < 2; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for i := 0; i < 15; i++ {
				key := fmt.Sprintf("%d-%d", worker, i)
				result, e := processor.Execute(ctx, application.WagerCommand{AuthorizedProviderID: "a", CorrelationID: key, Request: domain.WagerRequest{ProviderID: "a", ExternalTransactionID: key, IdempotencyKey: key, WalletID: state.ID, PlayerID: state.PlayerID, RoundID: "r", GameID: "g", Kind: domain.WagerBet, Money: amount}})
				if e != nil {
					failures <- e
					return
				}
				tx, e := result.Transaction.Snapshot()
				if e != nil || tx.Status != domain.WagerProcessed {
					failures <- fmt.Errorf("debit was not processed")
					return
				}
			}
		}(worker)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 100; i++ {
			result, e := audit.Execute(ctx, state.ID, "audit")
			if e != nil {
				failures <- e
				return
			}
			if !result.Consistent {
				failures <- fmt.Errorf("false discrepancy during writes")
				return
			}
		}
	}()
	close(start)
	wg.Wait()
	close(failures)
	for e := range failures {
		t.Error(e)
	}
	if t.Failed() {
		return
	}
	result, err := audit.Execute(ctx, state.ID, "final")
	if err != nil {
		t.Fatal(err)
	}
	expected, _ := domain.NewMoney(7000, "BRL")
	if !result.Consistent || result.StoredBalance != expected || result.CalculatedBalance != expected || result.CheckedEntries != 31 || audit.Discrepancies() != 0 {
		t.Fatal("unexpected final reconciliation")
	}
}
