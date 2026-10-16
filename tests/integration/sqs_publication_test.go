//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsadapter "github.com/evandromadeira/jungle-wallet-service/internal/adapters/sqs"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

// localSQS resolves only the Compose emulator; test credentials never reach AWS.
func localSQS(t *testing.T, ctx context.Context) *awssqs.Client {
	t.Helper()
	cmd := exec.CommandContext(ctx, "docker", "compose", "port", "localstack", "4566")
	cmd.Dir = filepath.Join("..", "..")
	b, e := cmd.Output()
	if e != nil {
		t.Fatal("start localstack with docker compose up -d --wait localstack")
	}
	endpoint := "http://" + strings.TrimSpace(string(b))
	return awssqs.New(awssqs.Options{Region: "us-east-1", BaseEndpoint: aws.String(endpoint), Credentials: credentials.NewStaticCredentialsProvider("test", "test", ""), RetryMaxAttempts: 1})
}

// TestSQSPublication exercises the real SDK, PostgreSQL and LocalStack, including replay.
func TestSQSPublication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client := localSQS(t, ctx)
	// Startup provisioning is part of the local environment contract.
	if _, e := client.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String("jungle-wallet-events")}); e != nil {
		t.Fatal(e)
	}
	queue, e := client.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String(fmt.Sprintf("outbox-test-%d-%d", os.Getpid(), time.Now().UnixNano()))})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, e := client.DeleteQueue(c, &awssqs.DeleteQueueInput{QueueUrl: queue.QueueUrl}); e != nil {
			t.Error("delete disposable SQS queue:", e)
		}
	})
	sender, e := sqsadapter.NewSender(client, aws.ToString(queue.QueueUrl))
	if e != nil {
		t.Fatal(e)
	}
	store, peer := repositoryStores(t, "000002_wager_transactions.up.sql", "000003_wallet_ledger_entries.up.sql", "000004_outbox_events.up.sql")
	money, _ := domain.NewMoney(12345, "BRL")
	_, e = openingService(t, store, "sqs").Execute(ctx, application.OpenWalletCommand{PlayerID: "player", InitialBalance: money, CorrelationID: "trace-sqs"})
	if e != nil {
		t.Fatal(e)
	}
	publisher := func(work application.UnitOfWork) *application.OutboxPublisher {
		p, e := application.NewOutboxPublisher(work, sender, time.Now, application.OutboxPolicy{BatchSize: 2, SendTimeout: 5 * time.Second, InitialDelay: time.Second, MaxDelay: time.Minute, PollInterval: time.Second})
		if e != nil {
			t.Fatal(e)
		}
		return p
	}
	// Acceptance occurs, but SQL confirmation is deliberately rolled back.
	if n, e := publisher(rollbackDelivery{store}).RunOnce(ctx); e == nil || n != 0 {
		t.Fatalf("missing interruption: %d %v", n, e)
	}
	// A different pool resumes the unpublished events.
	if n, e := publisher(peer).RunOnce(ctx); e != nil || n != 2 {
		t.Fatalf("publish: %d %v", n, e)
	}
	if n, e := publisher(peer).RunOnce(ctx); e != nil || n != 0 {
		t.Fatalf("published event still due: %d %v", n, e)
	}
	bodies := map[string]string{}
	counts := map[string]int{}
	received := 0
	for received < 3 {
		out, e := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{QueueUrl: queue.QueueUrl, MaxNumberOfMessages: 10, WaitTimeSeconds: 1, VisibilityTimeout: 30})
		if e != nil {
			t.Fatal(e)
		}
		for _, m := range out.Messages {
			var envelope map[string]any
			body := aws.ToString(m.Body)
			if e := json.Unmarshal([]byte(body), &envelope); e != nil {
				t.Fatal(e)
			}
			id, _ := envelope["eventId"].(string)
			if id != "sqs-4" && id != "sqs-5" {
				t.Fatalf("unexpected event ID %q", id)
			}
			if counts[id] > 0 && bodies[id] != body {
				t.Fatal("republication changed the envelope")
			}
			bodies[id] = body
			counts[id]++
			received++
			e = peer.WithinTransaction(ctx, func(r application.Repositories) error {
				s, e := r.Outbox.Get(ctx, id)
				if e != nil {
					return e
				}
				var data any
				if e = json.Unmarshal([]byte(s.Payload), &data); e != nil {
					return e
				}
				if !reflect.DeepEqual(data, envelope["data"]) || envelope["correlationId"] != s.CorrelationID || envelope["eventType"] != s.EventType || envelope["aggregateId"] != s.AggregateID || envelope["version"] != float64(s.Version) {
					t.Error("stored snapshot differs from received event")
				}
				return nil
			})
			if e != nil {
				t.Fatal(e)
			}
			if _, e = client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{QueueUrl: queue.QueueUrl, ReceiptHandle: m.ReceiptHandle}); e != nil {
				t.Fatal(e)
			}
		}
	}
	if len(bodies) != 2 || counts["sqs-4"] < 2 {
		t.Fatalf("missing durable events or stable replay: %v", counts)
	}
	// A nonexistent queue must not be mistaken for acceptance.
	missing, _ := sqsadapter.NewSender(client, aws.ToString(queue.QueueUrl)+"-missing")
	if e = missing.Send(ctx, []byte(`{"eventId":"unroutable"}`)); e == nil {
		t.Fatal("nonexistent destination accepted")
	}
}
