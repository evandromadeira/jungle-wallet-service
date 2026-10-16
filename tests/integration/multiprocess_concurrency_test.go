//go:build integration

package integration

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

type batchOutcome struct {
	ID     string
	Status domain.WagerStatus
	Replay bool
	Error  string
}

// runFinancialBatch releases all local requests after the parent arms every process.
func runFinancialBatch(t *testing.T, ctx context.Context, service *application.ProcessWager, scanner *bufio.Scanner) {
	t.Helper()
	var commands []application.WagerCommand
	if err := json.Unmarshal(scanner.Bytes(), &commands); err != nil {
		t.Fatal(err)
	}
	outcomes := make([]batchOutcome, len(commands))
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(len(commands))
	done.Add(len(commands))
	for i, command := range commands {
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			result, err := service.Execute(ctx, command)
			if err != nil {
				outcomes[i].Error = err.Error()
				return
			}
			state, err := result.Transaction.Snapshot()
			if err != nil {
				outcomes[i].Error = err.Error()
				return
			}
			outcomes[i] = batchOutcome{ID: state.ID, Status: state.Status, Replay: result.IdempotentReplay}
		}()
	}
	ready.Wait()
	fmt.Println("BATCH_READY")
	if !scanner.Scan() || scanner.Text() != "GO" {
		close(start)
		done.Wait()
		t.Fatal("missing batch release")
	}
	close(start)
	done.Wait()
	data, err := json.Marshal(outcomes)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("BATCH_RESULT %s\n", data)
}

// runThreeProcessBatch gathers every response without filling the child output channel.
func runThreeProcessBatch(t *testing.T, ctx context.Context, dsn string, batches [3][]application.WagerCommand) []batchOutcome {
	t.Helper()
	var children [3]*financialChild
	for i := range children {
		children[i] = startFinancialChild(t, ctx, dsn, "batch")
		if err := json.NewEncoder(children[i].input).Encode(batches[i]); err != nil {
			t.Fatal(err)
		}
	}
	for _, child := range children {
		child.expect(t, ctx, "BATCH_READY")
	}
	for _, child := range children {
		if _, err := fmt.Fprintln(child.input, "GO"); err != nil {
			t.Fatal(err)
		}
	}
	var outcomes []batchOutcome
	for i, child := range children {
		found := false
		for !found {
			select {
			case line, ok := <-child.lines:
				if !ok {
					t.Fatal("child exited without batch results")
				}
				if data, ok := strings.CutPrefix(line, "BATCH_RESULT "); ok {
					var batch []batchOutcome
					if err := json.Unmarshal([]byte(data), &batch); err != nil {
						t.Fatal(err)
					}
					if len(batch) != len(batches[i]) {
						t.Fatalf("wrong response count: %d", len(batch))
					}
					outcomes = append(outcomes, batch...)
					found = true
				}
			case <-ctx.Done():
				t.Fatal("batch result timeout")
			}
		}
		select {
		case err := <-child.done:
			if err != nil {
				t.Fatalf("batch process failed: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("batch exit timeout")
		}
	}
	return outcomes
}

// TestMultiprocessWagerBatches checks persistent idempotency and independent wallet balances.
func TestMultiprocessWagerBatches(t *testing.T) {
	store, dsn, adminDSN := restrictedStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	observer, err := pgxpool.New(ctx, adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	opening, err := application.NewOpenWallet(store, func() (string, error) { return rand.Text(), nil }, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	makeCommand := func(player string, debit int64) application.WagerCommand {
		t.Helper()
		initial, _ := domain.NewMoney(10000, "BRL")
		wallet, err := opening.Execute(ctx, application.OpenWalletCommand{PlayerID: player, InitialBalance: initial, CorrelationID: "open"})
		if err != nil {
			t.Fatal(err)
		}
		state, err := wallet.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		amount, _ := domain.NewMoney(debit, "BRL")
		return application.WagerCommand{AuthorizedProviderID: "a", CorrelationID: player, Request: domain.WagerRequest{ProviderID: "a", ExternalTransactionID: player, IdempotencyKey: player, WalletID: state.ID, PlayerID: player, RoundID: "r", GameID: "g", Kind: domain.WagerBet, Money: amount}}
	}
	assertWallet := func(command application.WagerCommand, balance int64, transactionID string) {
		t.Helper()
		var b, version, ledger, wagers, outbox int64
		err := observer.QueryRow(ctx, "SELECT balance_minor,version,(SELECT count(*) FROM public.wallet_ledger_entries WHERE wallet_id=$1),(SELECT count(*) FROM public.wager_transactions WHERE wallet_id=$1),(SELECT count(*) FROM public.outbox_events WHERE aggregate_id=$1 OR aggregate_id IN (SELECT id FROM public.wager_transactions WHERE wallet_id=$1)) FROM public.wallets WHERE id=$1", command.Request.WalletID).Scan(&b, &version, &ledger, &wagers, &outbox)
		if err != nil {
			t.Fatal(err)
		}
		if b != balance || version != 2 || ledger != 2 || wagers != 2 || outbox != 4 {
			t.Fatalf("unexpected wallet state: %d %d %d %d %d", b, version, ledger, wagers, outbox)
		}
		var id, status, failure string
		var resultBalance int64
		err = observer.QueryRow(ctx, "SELECT id,status,COALESCE(failure_code,''),result_balance_minor FROM public.wager_transactions WHERE wallet_id=$1 AND kind='BET' AND external_transaction_id=$2", command.Request.WalletID, command.Request.ExternalTransactionID).Scan(&id, &status, &failure, &resultBalance)
		if err != nil {
			t.Fatal(err)
		}
		if id != transactionID || status != "PROCESSED" || failure != "" || resultBalance != balance {
			t.Fatalf("unexpected persisted outcome: %s %s %s %d", id, status, failure, resultBalance)
		}
	}
	t.Run("50 identical requests across three processes", func(t *testing.T) {
		command := makeCommand("batch-identical", 8000)
		var batches [3][]application.WagerCommand
		for i := 0; i < 50; i++ {
			batches[i%3] = append(batches[i%3], command)
		}
		outcomes := runThreeProcessBatch(t, ctx, dsn, batches)
		ids := make(map[string]bool)
		replays := 0
		for _, outcome := range outcomes {
			if outcome.Error != "" || outcome.Status != domain.WagerProcessed || outcome.ID == "" {
				t.Fatalf("unexpected response: %+v", outcome)
			}
			ids[outcome.ID] = true
			if outcome.Replay {
				replays++
			}
		}
		if len(outcomes) != 50 || len(ids) != 1 || replays != 49 {
			t.Fatalf("responses=%d unique transactions=%d replays=%d", len(outcomes), len(ids), replays)
		}
		assertWallet(command, 2000, outcomes[0].ID)
	})
	t.Run("distinct wallets across three processes", func(t *testing.T) {
		var batches [3][]application.WagerCommand
		for i := range batches {
			batches[i] = []application.WagerCommand{makeCommand(fmt.Sprintf("batch-wallet-%d", i), int64(i+1)*1000)}
		}
		outcomes := runThreeProcessBatch(t, ctx, dsn, batches)
		ids := make(map[string]bool)
		for i, outcome := range outcomes {
			if outcome.Error != "" || outcome.Status != domain.WagerProcessed || outcome.Replay || outcome.ID == "" {
				t.Fatalf("unexpected response: %+v", outcome)
			}
			ids[outcome.ID] = true
			assertWallet(batches[i][0], 10000-int64(i+1)*1000, outcome.ID)
		}
		if len(ids) != 3 {
			t.Fatalf("expected three independent transactions, got %d", len(ids))
		}
	})
}
