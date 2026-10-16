//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/bootstrap"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestWorkerLifecycle starts the real Fx composition against disposable PostgreSQL/SQS resources.
func TestWorkerLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	store, dsn, adminDSN := restrictedStore(t)
	observer, e := pgxpool.New(ctx, adminDSN)
	if e != nil {
		t.Fatal("observer pool creation failed")
	}
	defer observer.Close()
	assertClosed := func() {
		t.Helper()
		// TCP close is local; PostgreSQL may observe it a few milliseconds later.
		deadline := time.NewTimer(2 * time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			var n int
			e := observer.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND application_name='jungle-wallet-worker'`).Scan(&n)
			if e != nil {
				t.Fatal("cannot inspect worker connections")
			}
			if n == 0 {
				return
			}
			select {
			case <-tick.C:
			case <-deadline.C:
				t.Fatalf("worker connections not released: %d", n)
			}
		}
	}
	client := localSQS(t, ctx)
	prefix := fmt.Sprintf("lifecycle-%d", time.Now().UnixNano())
	create := func(name string, attrs map[string]string) string {
		q, e := client.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String(name), Attributes: attrs})
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() {
			c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, e := client.DeleteQueue(c, &awssqs.DeleteQueueInput{QueueUrl: q.QueueUrl}); e != nil {
				t.Error("queue cleanup failed")
			}
		})
		return aws.ToString(q.QueueUrl)
	}
	dlq := create(prefix+"-dlq.fifo", map[string]string{"FifoQueue": "true"})
	a, e := client.GetQueueAttributes(ctx, &awssqs.GetQueueAttributesInput{QueueUrl: aws.String(dlq), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn}})
	if e != nil {
		t.Fatal(e)
	}
	policy, _ := json.Marshal(map[string]string{"deadLetterTargetArn": a.Attributes["QueueArn"], "maxReceiveCount": "5"})
	input := create(prefix+".fifo", map[string]string{"FifoQueue": "true", "RedrivePolicy": string(policy)})
	output := create(prefix+"-events", nil)
	config := bootstrap.Config{AWSRegion: "us-east-1", AWSAccessKeyID: "test", AWSSecretAccessKey: "test", DatabaseURL: dsn, SQSEndpoint: aws.ToString(client.Options().BaseEndpoint), InputQueueURL: input, OutputQueueURL: output, ProviderID: "a", ConsumerName: "wagers-a"}
	portCommand := exec.CommandContext(ctx, "docker", "compose", "port", "keycloak", "8080")
	portCommand.Dir = filepath.Join("..", "..")
	address, e := portCommand.Output()
	if e != nil {
		t.Fatal("Keycloak port unavailable")
	}
	config.OIDCIssuer = "http://" + strings.TrimSpace(string(address)) + "/realms/jungle-wallet"
	config.OIDCAudience = "wallet-api"
	reservation, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	config.HTTPAddress = reservation.Addr().String()
	_ = reservation.Close()
	bad := config
	bad.OutputQueueURL += "-missing"
	failed := bootstrap.New(bad)
	if e := failed.Err(); e != nil {
		t.Fatal(e)
	}
	if e := failed.Start(ctx); e == nil {
		_ = failed.Stop(ctx)
		t.Fatal("missing queue did not prevent startup")
	}
	assertClosed()
	amount, _ := domain.NewMoney(10000, "BRL")
	wallet, e := openingService(t, store, "lifecycle").Execute(ctx, application.OpenWalletCommand{PlayerID: "p", InitialBalance: amount, CorrelationID: "opening"})
	if e != nil {
		t.Fatal(e)
	}
	ws, _ := wallet.Snapshot()
	m := application.WagerMessage{MessageID: "lifecycle-bet", Type: "WagerTransactionRequested", OccurredAt: time.Now().UTC()}
	m.Data.ProviderID = "a"
	m.Data.ExternalTransactionID = "bet"
	m.Data.IdempotencyKey = "bet"
	m.Data.WalletID = ws.ID
	m.Data.PlayerID = "p"
	m.Data.RoundID = "r"
	m.Data.GameID = "g"
	m.Data.Kind = domain.WagerBet
	m.Data.Money, _ = domain.NewMoney(2500, "BRL")
	// Persist a WIN before its BET so the reference worker must complete it later.
	seq := 0
	processor, e := application.NewProcessWager(store, func() (string, error) { seq++; return fmt.Sprintf("runtime-reference-%d", seq), nil }, time.Now)
	if e != nil {
		t.Fatal(e)
	}
	win, _ := domain.NewMoney(1000, "BRL")
	pending, e := processor.Execute(ctx, application.WagerCommand{AuthorizedProviderID: "a", CorrelationID: "reference", Request: domain.WagerRequest{ProviderID: "a", ExternalTransactionID: "win", IdempotencyKey: "win", WalletID: ws.ID, PlayerID: "p", RoundID: "r", GameID: "g", Kind: domain.WagerWin, Money: win, ReferenceExternalTransactionID: "bet"}})
	if e != nil {
		t.Fatal(e)
	}
	pendingState, _ := pending.Transaction.Snapshot()
	if pendingState.Status != domain.WagerPendingReference {
		t.Fatal("reference was not pending")
	}
	body, _ := json.Marshal(m)
	if _, e = client.SendMessage(ctx, &awssqs.SendMessageInput{QueueUrl: aws.String(input), MessageBody: aws.String(string(body)), MessageGroupId: aws.String(ws.ID), MessageDeduplicationId: aws.String(m.MessageID)}); e != nil {
		t.Fatal(e)
	}
	app := bootstrap.New(config)
	if e := app.Err(); e != nil {
		t.Fatal(e)
	}
	if e := app.Start(ctx); e != nil {
		t.Fatal(e)
	}
	// Always stop before test database/queues are removed, even if an assertion fails.
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if e := app.Stop(c); e != nil {
			t.Error(e)
		}
	}()
	httpClient := &http.Client{Timeout: 10 * time.Second}
	for _, path := range []string{"/health/live", "/health/ready"} {
		req, err := http.NewRequestWithContext(ctx, "GET", "http://"+config.HTTPAddress+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		res, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != 200 {
			t.Fatalf("public probe %s returned %d", path, res.StatusCode)
		}
	}
	tokenRequest, e := http.NewRequestWithContext(ctx, "POST", config.OIDCIssuer+"/protocol/openid-connect/token", strings.NewReader(url.Values{"grant_type": {"client_credentials"}, "client_id": {"wallet-internal"}, "client_secret": {"local-wallet-internal-only"}}.Encode()))
	if e != nil {
		t.Fatal(e)
	}
	tokenRequest.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	tokenResponse, e := httpClient.Do(tokenRequest)
	if e != nil {
		t.Fatal("token request failed")
	}
	var credentials struct {
		AccessToken string `json:"access_token"`
	}
	e = json.NewDecoder(tokenResponse.Body).Decode(&credentials)
	tokenResponse.Body.Close()
	if e != nil || credentials.AccessToken == "" {
		t.Fatal("token unavailable")
	}
	// Zero opening leaves the expected financial event count unchanged.
	request, e := http.NewRequestWithContext(ctx, "POST", "http://"+config.HTTPAddress+"/wallets", strings.NewReader(`{"playerId":"http-runtime","initialBalance":{"amount":"0.00","currency":"BRL"}}`))
	if e != nil {
		t.Fatal(e)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+credentials.AccessToken)
	response, e := httpClient.Do(request)
	if e != nil {
		t.Fatal("runtime HTTP request failed")
	}
	response.Body.Close()
	if response.StatusCode != 201 {
		t.Fatalf("runtime HTTP status %d", response.StatusCode)
	}
	metricRequest, e := http.NewRequestWithContext(ctx, "GET", "http://"+config.HTTPAddress+"/metrics", nil)
	if e != nil {
		t.Fatal(e)
	}
	metricRequest.Header.Set("Authorization", "Bearer "+credentials.AccessToken)
	metricResponse, e := httpClient.Do(metricRequest)
	if e != nil {
		t.Fatal(e)
	}
	metricBody, e := io.ReadAll(metricResponse.Body)
	metricResponse.Body.Close()
	if e != nil || metricResponse.StatusCode != 200 {
		t.Fatal("runtime metrics failed")
	}
	for _, name := range []string{"wallet_outbox_pending", "wallet_outbox_oldest_age_seconds", "wallet_dlq_visible_messages", "wallet_dlq_inflight_messages", "wallet_duplicates_total", "wallet_sqs_retries_total"} {
		if !strings.Contains(string(metricBody), name) {
			t.Fatalf("missing metric %s", name)
		}
	}
	seen := map[string]bool{}
	for len(seen) < 7 {
		events, e := client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{QueueUrl: aws.String(output), WaitTimeSeconds: 1, MaxNumberOfMessages: 10})
		if e != nil {
			t.Fatal(e)
		}
		for _, event := range events.Messages {
			var envelope struct {
				EventID string `json:"eventId"`
			}
			if e = json.Unmarshal([]byte(aws.ToString(event.Body)), &envelope); e != nil {
				t.Fatal(e)
			}
			seen[envelope.EventID] = true
			if _, e = client.DeleteMessage(ctx, &awssqs.DeleteMessageInput{QueueUrl: aws.String(output), ReceiptHandle: event.ReceiptHandle}); e != nil {
				t.Fatal(e)
			}
		}
	}
	w, e := store.GetWallet(ctx, ws.ID)
	if e != nil {
		t.Fatal(e)
	}
	state, _ := w.Snapshot()
	want, _ := domain.NewMoney(8500, "BRL")
	if state.Balance != want || state.Version != 3 {
		t.Fatal("worker did not process bet atomically")
	}
	stopCtx, stop := context.WithTimeout(ctx, 20*time.Second)
	defer stop()
	if e := app.Stop(stopCtx); e != nil {
		t.Fatal(e)
	}
	assertClosed()
	probe, e := net.DialTimeout("tcp", config.HTTPAddress, time.Second)
	if e == nil {
		probe.Close()
		t.Fatal("HTTP listener survived shutdown")
	}
	// A fresh application resumes existing state; a database fault must request exit code 1.
	next := bootstrap.New(config)
	if e := next.Start(ctx); e != nil {
		t.Fatal(e)
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if e := next.Stop(c); e != nil {
			t.Error(e)
		}
	}()
	shutdown := next.Wait()
	if _, e := observer.Exec(ctx, "ALTER TABLE public.outbox_events RENAME TO unavailable_outbox"); e != nil {
		t.Fatal(e)
	}
	select {
	case signal := <-shutdown:
		if signal.ExitCode != 1 {
			t.Fatalf("worker failure exit code %d", signal.ExitCode)
		}
	case <-ctx.Done():
		t.Fatal("worker failure did not stop application")
	}
	if e := next.Stop(stopCtx); e != nil {
		t.Fatal(e)
	}
	assertClosed()
	if _, e := observer.Exec(ctx, "ALTER TABLE public.unavailable_outbox RENAME TO outbox_events"); e != nil {
		t.Fatal(e)
	}
}
