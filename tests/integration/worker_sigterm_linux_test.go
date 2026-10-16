//go:build integration && linux

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestWorkerSIGTERM runs the production entry point with race instrumentation on Linux.
// A real database lock provides the barrier; no production shutdown hooks are replaced.
func TestWorkerSIGTERM(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "wallet-worker")
	build := exec.CommandContext(ctx, "go", "build", "-race", "-o", binary, "./cmd/wallet-worker")
	build.Dir = filepath.Join("..", "..")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build worker: %v: %s", err, output)
	}
	store, dsn, adminDSN := restrictedStore(t)
	observer, err := pgxpool.New(ctx, adminDSN)
	if err != nil {
		t.Fatal("observer pool failed")
	}
	defer observer.Close()
	client := localSQS(t, ctx)
	prefix := fmt.Sprintf("sigterm-%d", time.Now().UnixNano())
	create := func(name string, attrs map[string]string) string {
		t.Helper()
		q, err := client.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attrs})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			if _, err := client.DeleteQueue(cleanup, &awssqs.DeleteQueueInput{QueueUrl: q.QueueUrl}); err != nil {
				t.Error(err)
			}
		})
		return aws.ToString(q.QueueUrl)
	}
	dlq := create(prefix+"-dlq.fifo", map[string]string{"FifoQueue": "true"})
	attrs, err := client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{QueueUrl: aws.String(dlq), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
	if err != nil {
		t.Fatal(err)
	}
	policy, _ := json.Marshal(map[string]string{"deadLetterTargetArn": attrs.Attributes["QueueArn"], "maxReceiveCount": "5"})
	input := create(prefix+".fifo", map[string]string{"FifoQueue": "true", "RedrivePolicy": string(policy)})
	output := create(prefix+"-events", nil)
	port := exec.CommandContext(ctx, "docker", "compose", "port", "keycloak", "8080")
	port.Dir = filepath.Join("..", "..")
	address, err := port.Output()
	if err != nil {
		t.Fatal("Keycloak port unavailable")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	httpAddress := listener.Addr().String()
	listener.Close()
	env := append(os.Environ(),
		"WALLET_DATABASE_URL="+dsn, "AWS_REGION=us-east-1", "AWS_ACCESS_KEY_ID=test", "AWS_SECRET_ACCESS_KEY=test", "AWS_SESSION_TOKEN=",
		"WALLET_SQS_ENDPOINT="+aws.ToString(client.Options().BaseEndpoint),
		"WALLET_INPUT_QUEUE_URL="+input, "WALLET_OUTPUT_QUEUE_URL="+output,
		"WALLET_PROVIDER_ID=a", "WALLET_CONSUMER_NAME=sigterm",
		"WALLET_HTTP_ADDRESS="+httpAddress, "WALLET_OIDC_AUDIENCE=wallet-api",
		"WALLET_OIDC_ISSUER=http://"+strings.TrimSpace(string(address))+"/realms/jungle-wallet")
	wait := func(label string, limit time.Duration, check func() bool) {
		t.Helper()
		deadline := time.NewTimer(limit)
		defer deadline.Stop()
		tick := time.NewTicker(25 * time.Millisecond)
		defer tick.Stop()
		for {
			if check() {
				return
			}
			select {
			case <-ctx.Done():
				t.Fatalf("%s: test deadline", label)
			case <-deadline.C:
				t.Fatalf("%s: deadline", label)
			case <-tick.C:
			}
		}
	}
	start := func() func() {
		t.Helper()
		cmd := exec.CommandContext(ctx, binary)
		cmd.Env = env
		var logs bytes.Buffer
		cmd.Stdout, cmd.Stderr = &logs, &logs
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		var exitErr error
		go func() { exitErr = cmd.Wait(); close(done) }()
		// Test cleanup reaps children before queues and database are removed.
		t.Cleanup(func() {
			_ = cmd.Process.Kill()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("worker cleanup timeout")
			}
		})
		httpClient := &http.Client{Timeout: time.Second}
		wait("worker ready", 20*time.Second, func() bool {
			select {
			case <-done:
				t.Fatalf("worker exited during startup: %v: %s", exitErr, logs.String())
			default:
			}
			response, err := httpClient.Get("http://" + httpAddress + "/health/ready")
			if err != nil {
				return false
			}
			response.Body.Close()
			return response.StatusCode == 200
		})
		return func() {
			t.Helper()
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			select {
			case <-done:
				if exitErr != nil {
					t.Fatalf("SIGTERM exit: %v: %s", exitErr, logs.String())
				}
				if !strings.Contains(logs.String(), "workers stopped") {
					t.Fatalf("missing graceful shutdown marker: %s", logs.String())
				}
			case <-time.After(10 * time.Second):
				t.Fatal("SIGTERM did not terminate within 10 seconds")
			}
			connection, err := net.DialTimeout("tcp", httpAddress, time.Second)
			if err == nil {
				connection.Close()
				t.Fatal("HTTP listener survived SIGTERM")
			}
			wait("worker connections closed", 2*time.Second, func() bool {
				var n int
				if err := observer.QueryRow(ctx, "SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='jungle-wallet-worker'").Scan(&n); err != nil {
					t.Fatal(err)
				}
				return n == 0
			})
		}
	}
	initial, _ := domain.NewMoney(10000, "BRL")
	wallet, err := openingService(t, store, prefix).Execute(ctx, application.OpenWalletCommand{PlayerID: "p", InitialBalance: initial, CorrelationID: "open"})
	if err != nil {
		t.Fatal(err)
	}
	ws, err := wallet.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	send := func(id, group string) {
		t.Helper()
		m := application.WagerMessage{MessageID: id, Type: "WagerTransactionRequested", OccurredAt: time.Now().UTC()}
		m.Data.ProviderID, m.Data.ExternalTransactionID, m.Data.IdempotencyKey = "a", id, id
		m.Data.WalletID, m.Data.PlayerID, m.Data.RoundID, m.Data.GameID = ws.ID, "p", "r", "g"
		m.Data.Kind = domain.WagerBet
		m.Data.Money, _ = domain.NewMoney(2500, "BRL")
		body, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.SendMessage(ctx, &awssqs.SendMessageInput{QueueUrl: aws.String(input), MessageBody: aws.String(string(body)), MessageGroupId: aws.String(group), MessageDeduplicationId: aws.String(id)}); err != nil {
			t.Fatal(err)
		}
	}
	stop := start()
	// Keep the row locked until the signaled process has exited and rollback is verified.
	lock, err := observer.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		_ = lock.Rollback(cleanup)
	}()
	if _, err := lock.Exec(ctx, "SELECT id FROM public.wallets WHERE id=$1 FOR UPDATE", ws.ID); err != nil {
		t.Fatal(err)
	}
	var blocker int
	if err := lock.QueryRow(ctx, "SELECT pg_backend_pid()").Scan(&blocker); err != nil {
		t.Fatal(err)
	}
	send("interrupted", "first")
	wait("worker blocked on wallet", 10*time.Second, func() bool {
		var blocked bool
		if err := observer.QueryRow(ctx, "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE datname=current_database() AND application_name='jungle-wallet-worker' AND $1=ANY(pg_blocking_pids(pid)))", blocker).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		return blocked
	})
	// A separate FIFO group is eligible immediately; it must remain unreceived at shutdown.
	send("queued", "second")
	stop()
	var balance, version, wagers, ledger, inbox, outbox int64
	if err := observer.QueryRow(ctx, "SELECT balance_minor,version,(SELECT count(*) FROM public.wager_transactions WHERE wallet_id=$1),(SELECT count(*) FROM public.wallet_ledger_entries WHERE wallet_id=$1),(SELECT count(*) FROM public.inbox_messages),(SELECT count(*) FROM public.outbox_events) FROM public.wallets WHERE id=$1", ws.ID).Scan(&balance, &version, &wagers, &ledger, &inbox, &outbox); err != nil {
		t.Fatal(err)
	}
	if balance != 10000 || version != 1 || wagers != 1 || ledger != 1 || inbox != 0 || outbox != 2 {
		t.Fatalf("partial writes after SIGTERM: %d %d %d %d %d %d", balance, version, wagers, ledger, inbox, outbox)
	}
	// Five seconds is below the configured 60-second visibility: this proves explicit release.
	receipts := map[string]*string{}
	receiveCtx, finishReceive := context.WithTimeout(ctx, 5*time.Second)
	defer finishReceive()
	for len(receipts) < 2 {
		messages, err := client.ReceiveMessage(receiveCtx, &awssqs.ReceiveMessageInput{QueueUrl: aws.String(input), WaitTimeSeconds: 1, MaxNumberOfMessages: 2, VisibilityTimeout: 30, MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount}})
		if err != nil {
			t.Fatalf("released messages unavailable: %v", err)
		}
		for _, message := range messages.Messages {
			var m application.WagerMessage
			if err := json.Unmarshal([]byte(aws.ToString(message.Body)), &m); err != nil {
				t.Fatal(err)
			}
			expected := map[string]string{"interrupted": "2", "queued": "1"}[m.MessageID]
			if expected == "" || message.Attributes["ApproximateReceiveCount"] != expected {
				t.Fatalf("unexpected receive count for %s: %v", m.MessageID, message.Attributes)
			}
			if _, exists := receipts[m.MessageID]; exists {
				t.Fatal("duplicate receipt during release verification")
			}
			receipts[m.MessageID] = message.ReceiptHandle
		}
	}
	if err := lock.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	for _, receipt := range receipts {
		if _, err := client.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{QueueUrl: aws.String(input), ReceiptHandle: receipt, VisibilityTimeout: 0}); err != nil {
			t.Fatal(err)
		}
	}
	stopRecovered := start()
	wait("both messages committed", 15*time.Second, func() bool {
		var n int
		if err := observer.QueryRow(ctx, "SELECT count(*) FROM public.inbox_messages WHERE consumer_name='sigterm' AND completed_at IS NOT NULL").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n == 2
	})
	// Wait for ACK, not merely database commit, before stopping the recovered worker.
	wait("input messages acknowledged", 5*time.Second, func() bool {
		a, err := client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{QueueUrl: aws.String(input), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameApproximateNumberOfMessages, types.QueueAttributeNameApproximateNumberOfMessagesNotVisible}})
		if err != nil {
			t.Fatal(err)
		}
		return a.Attributes["ApproximateNumberOfMessages"] == "0" && a.Attributes["ApproximateNumberOfMessagesNotVisible"] == "0"
	})
	stopRecovered()
	var processed int64
	if err := observer.QueryRow(ctx, "SELECT balance_minor,version,(SELECT count(*) FROM public.wager_transactions WHERE wallet_id=$1),(SELECT count(*) FROM public.wallet_ledger_entries WHERE wallet_id=$1),(SELECT count(*) FROM public.wager_transactions WHERE wallet_id=$1 AND kind='BET' AND status='PROCESSED' AND failure_code IS NULL),(SELECT count(*) FROM public.outbox_events) FROM public.wallets WHERE id=$1", ws.ID).Scan(&balance, &version, &wagers, &ledger, &processed, &outbox); err != nil {
		t.Fatal(err)
	}
	if balance != 5000 || version != 3 || wagers != 3 || ledger != 3 || processed != 2 || outbox != 6 {
		t.Fatalf("unexpected recovered state: %d %d %d %d %d %d", balance, version, wagers, ledger, processed, outbox)
	}
	remaining, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{QueueUrl: aws.String(input), WaitTimeSeconds: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining.Messages) != 0 {
		t.Fatalf("input not drained: %v (%d messages)", err, len(remaining.Messages))
	}
	dead, err := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{QueueUrl: aws.String(dlq), WaitTimeSeconds: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(dead.Messages) != 0 {
		t.Fatalf("unexpected dead letters: %v (%d messages)", err, len(dead.Messages))
	}
}
