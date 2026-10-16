//go:build integration

package integration

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/evandromadeira/jungle-wallet-service/internal/adapters/postgres"
	sqsadapter "github.com/evandromadeira/jungle-wallet-service/internal/adapters/sqs"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
	"testing"
	"time"
)

type pauseSender struct{ application.EventSender }

func (s pauseSender) Send(ctx context.Context, body []byte) error {
	if err := s.EventSender.Send(ctx, body); err != nil {
		return err
	}
	fmt.Println("BROKER_ACCEPTED")
	<-ctx.Done()
	return ctx.Err()
}

// transportProcess uses real adapters with test-only barriers at acknowledgement boundaries.
func transportProcess(t *testing.T, ctx context.Context, store *postgres.Store, mode string, raw []byte) {
	var queue string
	if json.Unmarshal(raw, &queue) != nil {
		t.Fatal("invalid queue")
	}
	broker := localSQS(t, ctx)
	if mode == "ack-crash" || mode == "ack-retry" {
		processor, err := application.NewProcessWager(store, func() (string, error) { return rand.Text(), nil }, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		handler := messageHandlerFunc(func(c context.Context, n, p, id string, b []byte) (application.WagerResult, error) {
			result, e := processor.HandleMessage(c, n, p, id, b)
			if e != nil {
				return result, e
			}
			if mode == "ack-crash" {
				fmt.Println("COMMITTED_NO_ACK")
				<-c.Done()
				return result, c.Err()
			}
			if !result.IdempotentReplay {
				return result, fmt.Errorf("expected durable replay")
			}
			return result, nil
		})
		consumer, err := sqsadapter.NewConsumer(broker, handler, sqsadapter.ConsumerPolicy{QueueURL: queue, ConsumerName: "process-crash", ProviderID: "a", WaitSeconds: 1, VisibilitySeconds: 15, ProcessingTimeout: 5 * time.Second, InitialRetrySeconds: 1, MaxRetrySeconds: 2})
		if err != nil {
			t.Fatal(err)
		}
		for ctx.Err() == nil {
			result, e := consumer.RunOnce(ctx)
			if e != nil {
				t.Fatal(e)
			}
			if result.Completed {
				fmt.Println("ACKED_REPLAY")
				return
			}
		}
		t.Fatal("consumer timed out")
	}
	sender, err := sqsadapter.NewSender(broker, queue)
	if err != nil {
		t.Fatal(err)
	}
	var send application.EventSender = sender
	if mode == "publish-crash" {
		send = pauseSender{sender}
	}
	publisher, err := application.NewOutboxPublisher(store, send, time.Now, application.OutboxPolicy{BatchSize: 100, SendTimeout: 20 * time.Second, InitialDelay: time.Second, MaxDelay: time.Minute, PollInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := publisher.RunOnce(ctx); err != nil {
		t.Fatal(err)
	}
	fmt.Println("PUBLISHED")
}

// TestTransportProcessRecovery proves durable redelivery and stable event envelopes after hard kills.
func TestTransportProcessRecovery(t *testing.T) {
	store, dsn, adminDSN := restrictedStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	broker := localSQS(t, ctx)
	queue := func(suffix string, fifo bool) string {
		t.Helper()
		attrs := map[string]string{}
		if fifo {
			attrs["FifoQueue"] = "true"
		}
		q, err := broker.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String(fmt.Sprintf("crash-%d-%s", time.Now().UnixNano(), suffix)), Attributes: attrs})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
			defer done()
			if _, err := broker.DeleteQueue(cleanup, &awssqs.DeleteQueueInput{QueueUrl: q.QueueUrl}); err != nil {
				t.Error(err)
			}
		})
		return aws.ToString(q.QueueUrl)
	}
	input := queue("input.fifo", true)
	output := queue("output", false)
	opening, err := application.NewOpenWallet(store, func() (string, error) { return rand.Text(), nil }, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	money, _ := domain.NewMoney(10000, "BRL")
	wallet, err := opening.Execute(ctx, application.OpenWalletCommand{PlayerID: "transport", InitialBalance: money, CorrelationID: "open"})
	if err != nil {
		t.Fatal(err)
	}
	state, _ := wallet.Snapshot()
	m := application.WagerMessage{MessageID: "hard-kill", Type: "WagerTransactionRequested", OccurredAt: time.Now().UTC()}
	m.Data.ProviderID = "a"
	m.Data.ExternalTransactionID = "hard-kill"
	m.Data.IdempotencyKey = "hard-kill"
	m.Data.PlayerID = "transport"
	m.Data.WalletID = state.ID
	m.Data.RoundID = "r"
	m.Data.GameID = "g"
	m.Data.Kind = domain.WagerBet
	m.Data.Money, _ = domain.NewMoney(2500, "BRL")
	body, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := broker.SendMessage(ctx, &awssqs.SendMessageInput{QueueUrl: aws.String(input), MessageBody: aws.String(string(body)), MessageGroupId: aws.String(state.ID), MessageDeduplicationId: aws.String(m.MessageID)}); err != nil {
		t.Fatal(err)
	}
	launch := func(mode, q string) *financialChild {
		t.Helper()
		c := startFinancialChild(t, ctx, dsn, mode)
		if err := json.NewEncoder(c.input).Encode(q); err != nil {
			t.Fatal(err)
		}
		return c
	}
	kill := func(c *financialChild) {
		t.Helper()
		if err := c.cmd.Process.Kill(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-c.done:
		case <-ctx.Done():
			t.Fatal("kill timeout")
		}
	}
	wait := func(c *financialChild) {
		t.Helper()
		select {
		case err := <-c.done:
			if err != nil {
				t.Fatal("recovery child failed")
			}
		case <-ctx.Done():
			t.Fatal("recovery timeout")
		}
	}
	child := launch("ack-crash", input)
	child.expect(t, ctx, "COMMITTED_NO_ACK")
	kill(child)
	retry := launch("ack-retry", input)
	retry.expect(t, ctx, "ACKED_REPLAY")
	wait(retry)
	w, err := store.GetWallet(ctx, state.ID)
	if err != nil {
		t.Fatal(err)
	}
	ws, _ := w.Snapshot()
	expected, _ := domain.NewMoney(7500, "BRL")
	if ws.Balance != expected || ws.Version != 2 {
		t.Fatal("redelivery duplicated debit")
	}
	remaining, err := broker.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{QueueUrl: aws.String(input), WaitTimeSeconds: 1})
	if err != nil || len(remaining.Messages) != 0 {
		t.Fatal("replay was not acknowledged")
	}
	child = launch("publish-crash", output)
	child.expect(t, ctx, "BROKER_ACCEPTED")
	kill(child)
	retry = launch("publish-retry", output)
	retry.expect(t, ctx, "PUBLISHED")
	wait(retry)
	counts := map[string]int{}
	bodies := map[string]string{}
	for received := 0; received < 5; {
		batch, err := broker.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{QueueUrl: aws.String(output), MaxNumberOfMessages: 10, WaitTimeSeconds: 1})
		if err != nil {
			t.Fatal(err)
		}
		for _, msg := range batch.Messages {
			var event struct{ EventID string }
			b := aws.ToString(msg.Body)
			if json.Unmarshal([]byte(b), &event) != nil || event.EventID == "" {
				t.Fatal("invalid event")
			}
			if old, ok := bodies[event.EventID]; ok && old != b {
				t.Fatal("replayed envelope changed")
			}
			bodies[event.EventID] = b
			counts[event.EventID]++
			received++
			if _, err := broker.DeleteMessage(ctx, &awssqs.DeleteMessageInput{QueueUrl: aws.String(output), ReceiptHandle: msg.ReceiptHandle}); err != nil {
				t.Fatal(err)
			}
		}
	}
	if len(counts) != 4 {
		t.Fatal("expected four unique events and a repeated send")
	}
	observer, err := pgxpool.New(ctx, adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	var ledger, transactions, published, inbox int
	if err := observer.QueryRow(ctx, "SELECT (SELECT count(*) FROM wallet_ledger_entries),(SELECT count(*) FROM wager_transactions),(SELECT count(*) FROM outbox_events WHERE published_at IS NOT NULL),(SELECT count(*) FROM inbox_messages WHERE completed_at IS NOT NULL)").Scan(&ledger, &transactions, &published, &inbox); err != nil {
		t.Fatal(err)
	}
	if ledger != 2 || transactions != 2 || published != 4 || inbox != 1 {
		t.Fatalf("unexpected durable counts %d %d %d %d", ledger, transactions, published, inbox)
	}
}
