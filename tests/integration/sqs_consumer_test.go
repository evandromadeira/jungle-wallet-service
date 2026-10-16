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

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	sqsadapter "github.com/evandromadeira/jungle-wallet-service/internal/adapters/sqs"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

type messageHandlerFunc func(context.Context, string, string, string, []byte) (application.WagerResult, error)

// HandleMessage allows an interruption immediately after the real database commit.
func (f messageHandlerFunc) HandleMessage(c context.Context, n, p, id string, b []byte) (application.WagerResult, error) {
	return f(c, n, p, id, b)
}

// TestSQSConsumer validates real receipt deletion, post-commit redelivery and broker redrive.
func TestSQSConsumer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client := localSQS(t, ctx)
	for _, name := range []string{"wager-transactions.fifo", "wager-transactions-dlq.fifo"} {
		if _, e := client.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String(name)}); e != nil {
			t.Fatal(e)
		}
	}
	create := func(name string, attrs map[string]string) string {
		q, e := client.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attrs})
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() {
			c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, e := client.DeleteQueue(c, &awssqs.DeleteQueueInput{QueueUrl: q.QueueUrl}); e != nil {
				t.Error(e)
			}
		})
		return aws.ToString(q.QueueUrl)
	}
	name := fmt.Sprintf("consumer-%d", time.Now().UnixNano())
	dlq := create(name+"-dlq.fifo", map[string]string{"FifoQueue": "true"})
	attrs, e := client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{QueueUrl: aws.String(dlq), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
	if e != nil {
		t.Fatal(e)
	}
	redrive, _ := json.Marshal(map[string]string{"deadLetterTargetArn": attrs.Attributes["QueueArn"], "maxReceiveCount": "2"})
	queue := create(name+".fifo", map[string]string{"FifoQueue": "true", "RedrivePolicy": string(redrive)})
	store, peer := repositoryStores(t, "000002_wager_transactions.up.sql", "000003_wallet_ledger_entries.up.sql", "000004_outbox_events.up.sql", "000005_inbox_messages.up.sql")
	money, _ := domain.NewMoney(10000, "BRL")
	wallet, e := openingService(t, store, "consumer").Execute(ctx, application.OpenWalletCommand{PlayerID: "p", InitialBalance: money, CorrelationID: "open"})
	if e != nil {
		t.Fatal(e)
	}
	ws, _ := wallet.Snapshot()
	var seq atomic.Int64
	processor := func(w application.UnitOfWork) *application.ProcessWager {
		p, e := application.NewProcessWager(w, func() (string, error) { return fmt.Sprintf("consume-%d", seq.Add(1)), nil }, time.Now)
		if e != nil {
			t.Fatal(e)
		}
		return p
	}
	first, second := processor(store), processor(peer)
	consumer := func(h sqsadapter.MessageHandler) *sqsadapter.Consumer {
		c, e := sqsadapter.NewConsumer(client, h, sqsadapter.ConsumerPolicy{QueueURL: queue, ConsumerName: "wagers-a", ProviderID: "a", WaitSeconds: 1, VisibilitySeconds: 20, ProcessingTimeout: 5 * time.Second, InitialRetrySeconds: 1, MaxRetrySeconds: 2})
		if e != nil {
			t.Fatal(e)
		}
		return c
	}
	body := func(id, provider string, amount int64) []byte {
		m := application.WagerMessage{MessageID: id, Type: "WagerTransactionRequested", OccurredAt: time.Now().UTC()}
		m.Data.ProviderID = provider
		m.Data.ExternalTransactionID = id
		m.Data.IdempotencyKey = id
		m.Data.WalletID = ws.ID
		m.Data.PlayerID = "p"
		m.Data.RoundID = "r"
		m.Data.GameID = "g"
		m.Data.Kind = domain.WagerBet
		m.Data.Money, _ = domain.NewMoney(amount, "BRL")
		b, e := json.Marshal(m)
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	send := func(id string, b []byte) {
		_, e := client.SendMessage(ctx, &awssqs.SendMessageInput{QueueUrl: aws.String(queue), MessageBody: aws.String(string(b)), MessageGroupId: aws.String(ws.ID), MessageDeduplicationId: aws.String(id)})
		if e != nil {
			t.Fatal(e)
		}
	}
	send("bet", body("bet", "a", 2500))
	interrupted, stop := context.WithCancel(ctx)
	h := messageHandlerFunc(func(c context.Context, n, p, id string, b []byte) (application.WagerResult, error) {
		r, e := first.HandleMessage(c, n, p, id, b)
		if e == nil {
			stop()
		}
		return r, e
	})
	if _, e := consumer(h).RunOnce(interrupted); !errors.Is(e, context.Canceled) {
		t.Fatalf("shutdown after commit: %v", e)
	}
	replays := 0
	h = func(c context.Context, n, p, id string, b []byte) (application.WagerResult, error) {
		r, e := second.HandleMessage(c, n, p, id, b)
		if r.IdempotentReplay {
			replays++
		}
		return r, e
	}
	if r, e := consumer(h).RunOnce(ctx); e != nil || !r.Completed || replays != 1 {
		t.Fatalf("durable replay: %+v %v replays=%d", r, e, replays)
	}
	// A committed business rejection is acknowledged too.
	send("reject", body("reject", "a", 99999))
	if r, e := consumer(second).RunOnce(ctx); e != nil || !r.Completed {
		t.Fatalf("rejection: %+v %v", r, e)
	}
	if r, e := consumer(second).RunOnce(ctx); e != nil || r.Received {
		t.Fatalf("acknowledged messages remain: %+v %v", r, e)
	}
	// A transient failure keeps the receipt; a later successful attempt debits once.
	send("transient", body("transient", "a", 100))
	failOnce := messageHandlerFunc(func(context.Context, string, string, string, []byte) (application.WagerResult, error) {
		return application.WagerResult{}, errors.New("injected database outage")
	})
	if r, e := consumer(failOnce).RunOnce(ctx); e != nil || !r.Retried {
		t.Fatalf("transient retry: %+v %v", r, e)
	}
	completed := false
	for !completed {
		r, e := consumer(second).RunOnce(ctx)
		if e != nil {
			t.Fatal(e)
		}
		completed = r.Completed
	}
	// Malformed content and unauthorized provider must exhaust receives into the DLQ.
	for _, poison := range []struct {
		id string
		b  []byte
	}{{"malformed", []byte(`{broken`)}, {"forged", body("forged", "b", 100)}} {
		send(poison.id, poison.b)
		retries := 0
		delivered := false
		for !delivered {
			r, e := consumer(second).RunOnce(ctx)
			if e != nil {
				t.Fatal(e)
			}
			if r.Completed {
				t.Fatal("poison acknowledged")
			}
			if r.Retried {
				retries++
			}
			dead, e := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{QueueUrl: aws.String(dlq), WaitTimeSeconds: 1})
			if e != nil {
				t.Fatal(e)
			}
			if len(dead.Messages) > 0 {
				if aws.ToString(dead.Messages[0].Body) != string(poison.b) {
					t.Fatal("DLQ body changed")
				}
				if _, e := client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{QueueUrl: aws.String(dlq), ReceiptHandle: dead.Messages[0].ReceiptHandle}); e != nil {
					t.Fatal(e)
				}
				delivered = true
			}
		}
		if retries != 2 {
			t.Fatalf("expected two retries, got %d", retries)
		}
	}
	e = peer.WithinTransaction(ctx, func(r application.Repositories) error {
		w, e := r.Wallets.Get(ctx, ws.ID)
		if e != nil {
			return e
		}
		s, _ := w.Snapshot()
		expected, _ := domain.NewMoney(7400, "BRL")
		if s.Balance != expected || s.Version != 3 {
			t.Error("redelivery or poison altered balance")
		}
		receipt, e := r.Inbox.GetForUpdate(ctx, "wagers-a", "bet")
		if e == nil && receipt.CompletedAt == nil {
			t.Error("receipt not completed")
		}
		return e
	})
	if e != nil {
		t.Fatal(e)
	}
}
