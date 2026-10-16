//go:build integration

package integration

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/adapters/postgres"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

// crashWork pauses after all transactional writes but before the callback permits commit.
type crashWork struct{ application.UnitOfWork }

func (w crashWork) WithinTransaction(ctx context.Context, fn func(application.Repositories) error) error {
	return w.UnitOfWork.WithinTransaction(ctx, func(r application.Repositories) error {
		if err := fn(r); err != nil {
			return err
		}
		fmt.Println("BEFORE_COMMIT")
		<-ctx.Done()
		return ctx.Err()
	})
}

// TestFinancialProcessHelper runs only when explicitly launched by the parent test.
func TestFinancialProcessHelper(t *testing.T) {
	if os.Getenv("WALLET_PROCESS_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, os.Getenv("WALLET_PROCESS_DSN"))
	if err != nil {
		t.Fatal("pool failed")
	}
	defer pool.Close()
	store, err := postgres.NewStore(pool)
	if err != nil {
		t.Fatal(err)
	}
	var work application.UnitOfWork = store
	mode := os.Getenv("WALLET_PROCESS_MODE")
	if mode == "before" {
		work = crashWork{store}
	}
	service, err := application.NewProcessWager(work, func() (string, error) { return rand.Text(), nil }, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("READY")
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		t.Fatal("missing start signal")
	}
	if mode == "ack-crash" || mode == "ack-retry" || mode == "publish-crash" || mode == "publish-retry" {
		transportProcess(t, ctx, store, mode, scanner.Bytes())
		return
	}
	if mode == "batch" {
		runFinancialBatch(t, ctx, service, scanner)
		return
	}
	var command application.WagerCommand
	if json.Unmarshal(scanner.Bytes(), &command) != nil {
		t.Fatal("invalid command")
	}
	result, err := service.Execute(ctx, command)
	if err != nil {
		t.Fatal("processing failed")
	}
	state, err := result.Transaction.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Printf("RESULT %s %t\n", state.Status, result.IdempotentReplay)
	if mode == "after" {
		fmt.Println("AFTER_COMMIT")
		<-ctx.Done()
	}
}

type financialChild struct {
	cmd   *exec.Cmd
	input io.WriteCloser
	lines chan string
	done  chan error
}

// startFinancialChild owns cleanup even if the parent assertion fails.
func startFinancialChild(t *testing.T, ctx context.Context, dsn, mode string) *financialChild {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestFinancialProcessHelper$", "-test.timeout=50s")
	cmd.Env = append(os.Environ(), "WALLET_PROCESS_HELPER=1", "WALLET_PROCESS_DSN="+dsn, "WALLET_PROCESS_MODE="+mode)
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	child := &financialChild{cmd: cmd, input: input, lines: make(chan string, 32), done: make(chan error, 1)}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var scan sync.WaitGroup
	scan.Add(1)
	go func() {
		defer scan.Done()
		defer close(child.lines)
		reader := bufio.NewScanner(output)
		for reader.Scan() {
			child.lines <- reader.Text()
		}
	}()
	go func() { scan.Wait(); child.done <- cmd.Wait() }()
	t.Cleanup(func() { input.Close(); _ = cmd.Process.Kill() })
	child.expect(t, ctx, "READY")
	return child
}

// expect waits for a protocol marker under the test deadline.
func (c *financialChild) expect(t *testing.T, ctx context.Context, want string) {
	t.Helper()
	for {
		select {
		case line, ok := <-c.lines:
			if !ok {
				t.Fatalf("child exited before %s", want)
			}
			if line == want {
				return
			}
		case <-ctx.Done():
			t.Fatalf("child timed out before %s", want)
		}
	}
}

// send releases a child waiting at the start barrier.
func (c *financialChild) send(t *testing.T, command application.WagerCommand) {
	t.Helper()
	if err := json.NewEncoder(c.input).Encode(command); err != nil {
		t.Fatal(err)
	}
}

// TestMultiprocessFinancialRecovery verifies contention and abrupt process loss around commit.
func TestMultiprocessFinancialRecovery(t *testing.T) {
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
	amount, _ := domain.NewMoney(10000, "BRL")
	makeCommand := func(player, key string) application.WagerCommand {
		t.Helper()
		wallet, e := opening.Execute(ctx, application.OpenWalletCommand{PlayerID: player, InitialBalance: amount, CorrelationID: "open"})
		if e != nil {
			t.Fatal(e)
		}
		state, _ := wallet.Snapshot()
		bet, _ := domain.NewMoney(8000, "BRL")
		return application.WagerCommand{AuthorizedProviderID: "a", CorrelationID: key, Request: domain.WagerRequest{ProviderID: "a", ExternalTransactionID: key, IdempotencyKey: key, WalletID: state.ID, PlayerID: player, RoundID: "r", GameID: "g", Kind: domain.WagerBet, Money: bet}}
	}
	assertState := func(id string, balance, version, ledger, wagers, outbox int64) {
		t.Helper()
		var b, v, l, w, o int64
		err := observer.QueryRow(ctx, "SELECT balance_minor,version,(SELECT count(*) FROM public.wallet_ledger_entries WHERE wallet_id=$1),(SELECT count(*) FROM public.wager_transactions WHERE wallet_id=$1),(SELECT count(*) FROM public.outbox_events WHERE aggregate_id=$1 OR aggregate_id IN (SELECT id FROM public.wager_transactions WHERE wallet_id=$1)) FROM public.wallets WHERE id=$1", id).Scan(&b, &v, &l, &w, &o)
		if err != nil {
			t.Fatal(err)
		}
		if b != balance || v != version || l != ledger || w != wagers || o != outbox {
			t.Fatalf("unexpected persisted totals: %d %d %d %d %d", b, v, l, w, o)
		}
	}
	first := makeCommand("multiprocess-contention", "contend-a")
	second := first
	second.Request.ExternalTransactionID = "contend-b"
	second.Request.IdempotencyKey = "contend-b"
	a := startFinancialChild(t, ctx, dsn, "normal")
	b := startFinancialChild(t, ctx, dsn, "normal")
	duplicate := startFinancialChild(t, ctx, dsn, "normal")
	a.send(t, first)
	b.send(t, second)
	duplicate.send(t, first)
	for _, c := range []*financialChild{a, b, duplicate} {
		select {
		case err := <-c.done:
			if err != nil {
				t.Fatal("contending process failed")
			}
		case <-ctx.Done():
			t.Fatal("contention timeout")
		}
	}
	// OPENING, one processed BET and one rejected BET; only two ledger entries.
	assertState(first.Request.WalletID, 2000, 2, 2, 3, 5)
	var processed, rejected int
	err = observer.QueryRow(ctx, "SELECT count(*) FILTER (WHERE status='PROCESSED' AND failure_code IS NULL), count(*) FILTER (WHERE status='REJECTED' AND failure_code='INSUFFICIENT_FUNDS') FROM public.wager_transactions WHERE wallet_id=$1 AND kind='BET' AND external_transaction_id IN ('contend-a','contend-b')", first.Request.WalletID).Scan(&processed, &rejected)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 1 || rejected != 1 {
		t.Fatalf("expected one processed bet and one insufficient-funds rejection; got %d and %d", processed, rejected)
	}
	for _, mode := range []string{"before", "after"} {
		command := makeCommand("crash-"+mode, "crash-"+mode)
		child := startFinancialChild(t, ctx, dsn, mode)
		child.send(t, command)
		marker := "BEFORE_COMMIT"
		if mode == "after" {
			marker = "AFTER_COMMIT"
		}
		child.expect(t, ctx, marker)
		if err := child.cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-child.done:
		case <-ctx.Done():
			t.Fatal("killed child did not exit")
		}
		retry := startFinancialChild(t, ctx, dsn, "normal")
		retry.send(t, command)
		expected := "RESULT PROCESSED false"
		if mode == "after" {
			expected = "RESULT PROCESSED true"
		}
		retry.expect(t, ctx, expected)
		select {
		case err := <-retry.done:
			if err != nil {
				t.Fatal("retry process failed")
			}
		case <-ctx.Done():
			t.Fatal("retry timeout")
		}
		assertState(command.Request.WalletID, 2000, 2, 2, 2, 4)
	}
}
