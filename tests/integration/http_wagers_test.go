//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqsadapter "github.com/evandromadeira/jungle-wallet-service/internal/adapters/sqs"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

// testHTTPWagers exercises the real HTTP route and a real FIFO re-delivery of its operation.
func testHTTPWagers(t *testing.T, ctx context.Context, client *http.Client, base string, tokens map[string]string, processor *application.ProcessWager, walletID string) {
	t.Helper()
	m := application.WagerMessage{MessageID: "http-sqs-replay", Type: "WagerTransactionRequested", OccurredAt: time.Now().UTC()}
	m.Data.ProviderID = "provider-a"
	m.Data.ExternalTransactionID = "http-bet"
	m.Data.IdempotencyKey = "http-key"
	m.Data.WalletID = walletID
	m.Data.PlayerID = "http-wager"
	m.Data.RoundID = "r"
	m.Data.GameID = "g"
	m.Data.Kind = domain.WagerBet
	m.Data.Money, _ = domain.NewMoney(2500, "BRL")
	body := func() string {
		b, _ := json.Marshal(m.Data)
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(b, &fields)
		delete(fields, "idempotencyKey")
		b, _ = json.Marshal(fields)
		return string(b)
	}
	type outcome struct {
		ID      string       `json:"transactionId"`
		Balance domain.Money `json:"balance"`
		Replay  bool         `json:"idempotentReplay"`
		Status  string       `json:"status"`
		Failure string       `json:"failureCode"`
	}
	send := func(token, key string, want int) outcome {
		t.Helper()
		req, e := http.NewRequestWithContext(ctx, "POST", base+"/wagering/transactions", strings.NewReader(body()))
		if e != nil {
			t.Fatal(e)
		}
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if key != "" {
			req.Header.Set("Idempotency-Key", key)
		}
		res, e := client.Do(req)
		if e != nil {
			t.Fatal("wager HTTP request failed")
		}
		defer res.Body.Close()
		if res.StatusCode != want {
			t.Fatalf("wager HTTP status %d want %d", res.StatusCode, want)
		}
		var out outcome
		_ = json.NewDecoder(res.Body).Decode(&out)
		return out
	}
	send("", "http-key", 401)
	send(tokens["wallet-internal"], "http-key", 403)
	send(tokens["provider-b"], "http-key", 403)
	send(tokens["provider-a"], "", 400)
	original := send(tokens["provider-a"], "http-key", 200)
	expected, _ := domain.NewMoney(7500, "BRL")
	if original.Balance != expected {
		t.Fatal("unexpected initial wager balance")
	}
	if original.ID == "" || original.Replay || original.Status != "PROCESSED" {
		t.Fatal("invalid processing response")
	}
	replay := send(tokens["provider-a"], "alternate-key", 200)
	if replay.ID != original.ID || !replay.Replay || replay.Balance != original.Balance {
		t.Fatal("alternate key repeated financial operation")
	}
	// Re-deliver the exact business request through SQS, after the HTTP commit.
	broker := localSQS(t, ctx)
	q, e := broker.CreateQueue(ctx, &awssqs.CreateQueueInput{QueueName: aws.String(fmt.Sprintf("cross-http-%d.fifo", time.Now().UnixNano())), Attributes: map[string]string{"FifoQueue": "true"}})
	if e != nil {
		t.Fatal(e)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, e := broker.DeleteQueue(cleanup, &awssqs.DeleteQueueInput{QueueUrl: q.QueueUrl}); e != nil {
			t.Error("queue cleanup failed")
		}
	}()
	b, _ := json.Marshal(m)
	if _, e := broker.SendMessage(ctx, &awssqs.SendMessageInput{QueueUrl: q.QueueUrl, MessageBody: aws.String(string(b)), MessageGroupId: aws.String(walletID), MessageDeduplicationId: aws.String(m.MessageID)}); e != nil {
		t.Fatal(e)
	}
	replayed := false
	handler := messageHandlerFunc(func(c context.Context, n, p, id string, b []byte) (application.WagerResult, error) {
		r, e := processor.HandleMessage(c, n, p, id, b)
		replayed = r.IdempotentReplay
		return r, e
	})
	consumer, e := sqsadapter.NewConsumer(broker, handler, sqsadapter.ConsumerPolicy{QueueURL: aws.ToString(q.QueueUrl), ConsumerName: "http-cross", ProviderID: "provider-a", WaitSeconds: 1, VisibilitySeconds: 20, ProcessingTimeout: 5 * time.Second, InitialRetrySeconds: 1, MaxRetrySeconds: 2})
	if e != nil {
		t.Fatal(e)
	}
	if r, e := consumer.RunOnce(ctx); e != nil || !r.Completed || !replayed {
		t.Fatal("SQS did not replay committed HTTP outcome")
	}
	m.Data.Money, _ = domain.NewMoney(1000, "BRL")
	send(tokens["provider-a"], "http-key", 409)
	m.Data.ExternalTransactionID = "second-bet"
	second := send(tokens["provider-a"], "second-key", 200)
	expected, _ = domain.NewMoney(6500, "BRL")
	if second.Balance != expected {
		t.Fatal("cross-transport replay changed balance")
	}
	m.Data.ExternalTransactionID = "http-bet"
	m.Data.Money, _ = domain.NewMoney(2500, "BRL")
	replay = send(tokens["provider-a"], "http-key", 200)
	if replay.Balance != original.Balance || !replay.Replay {
		t.Fatal("replay used current balance")
	}
	m.Data.ExternalTransactionID = "reject"
	m.Data.Money, _ = domain.NewMoney(100000, "BRL")
	rejected := send(tokens["provider-a"], "reject-key", 422)
	if rejected.Failure != "INSUFFICIENT_FUNDS" {
		t.Fatal("missing business rejection")
	}
	m.Data.ExternalTransactionID = "pending"
	m.Data.Kind = domain.WagerWin
	m.Data.ReferenceExternalTransactionID = "absent-bet"
	m.Data.Money, _ = domain.NewMoney(100, "BRL")
	if send(tokens["provider-a"], "pending-key", 202).Status != "PENDING_REFERENCE" {
		t.Fatal("pending reference not represented")
	}
	m.Data.ReferenceExternalTransactionID = ""
	m.Data.Kind = domain.WagerBet
	m.Data.Money, _ = domain.NewMoney(0, "BRL")
	send(tokens["provider-a"], "zero-bet-key", 400)
	m.Data.Kind = domain.WagerRefund
	m.Data.Money, _ = domain.NewMoney(100, "BRL")
	send(tokens["provider-a"], "missing-reference-key", 400)
	// Wallet reads require internal identity and return the current committed balance.
	walletRead := func(token, id string, want int) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, "GET", base+"/wallets/"+id, nil)
		if err != nil {
			t.Fatal(err)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if res.StatusCode != want {
			t.Fatalf("wallet query status %d want %d", res.StatusCode, want)
		}
		if want != 200 {
			return
		}
		if res.Header.Get("Cache-Control") != "no-store" {
			t.Fatal("wallet response can be cached")
		}
		var got struct {
			ID        string
			PlayerID  string
			Balance   domain.Money
			Version   int64
			CreatedAt time.Time
			UpdatedAt time.Time
		}
		if err := json.NewDecoder(res.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		balance, _ := domain.NewMoney(6500, "BRL")
		if got.ID != walletID || got.PlayerID != "http-wager" || got.Balance != balance || got.Version != 3 || got.CreatedAt.IsZero() || got.UpdatedAt.Before(got.CreatedAt) {
			t.Fatal("wallet query did not return current committed state")
		}
	}
	walletRead("", walletID, 401)
	walletRead(tokens["provider-a"], walletID, 403)
	walletRead(tokens["provider-b"], walletID, 403)
	walletRead(tokens["provider-a"], "missing", 403)
	walletRead(tokens["wallet-internal"], "missing", 404)
	walletRead(tokens["wallet-internal"], "%20", 400)
	walletRead(tokens["wallet-internal"], walletID, 200)

	// Reconciliation reads the same committed wallet and all three ledger entries.
	reconcile := func(token, id string, want int) {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, "POST", base+"/wallets/"+id+"/reconciliation", nil)
		if err != nil {
			t.Fatal(err)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if res.StatusCode != want {
			t.Fatalf("reconciliation status %d want %d", res.StatusCode, want)
		}
		if want != 200 {
			return
		}
		var got struct {
			WalletID                                     string
			StoredBalance, CalculatedBalance, Difference domain.Money
			Consistent                                   bool
			CheckedEntries                               int64
		}
		if err := json.NewDecoder(res.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		balance, _ := domain.NewMoney(6500, "BRL")
		zero, _ := domain.NewMoney(0, "BRL")
		if got.WalletID != walletID || got.StoredBalance != balance || got.CalculatedBalance != balance || got.Difference != zero || !got.Consistent || got.CheckedEntries != 3 {
			t.Fatal("invalid reconciliation")
		}
	}
	reconcile("", walletID, 401)
	reconcile(tokens["provider-a"], walletID, 403)
	reconcile(tokens["wallet-internal"], "missing", 404)
	reconcile(tokens["wallet-internal"], walletID, 200)
	walletRead(tokens["wallet-internal"], walletID, 200)

	// Ledger pages preserve ordering and contain each committed movement exactly once.
	type ledgerPage struct {
		Entries []struct {
			ID           string
			WalletID     string
			BalanceAfter domain.Money
			CreatedAt    time.Time
		}
		NextCursor string
	}
	readLedger := func(token, path string, want int) ledgerPage {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, "GET", base+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if res.StatusCode != want {
			t.Fatalf("ledger status %d want %d", res.StatusCode, want)
		}
		var page ledgerPage
		if want == 200 {
			if err := json.NewDecoder(res.Body).Decode(&page); err != nil {
				t.Fatal(err)
			}
		}
		return page
	}
	ledgerPath := "/wallets/" + walletID + "/ledger"
	readLedger("", ledgerPath, 401)
	readLedger(tokens["provider-a"], ledgerPath, 403)
	readLedger(tokens["wallet-internal"], "/wallets/missing/ledger", 404)
	for _, suffix := range []string{"?limit=0", "?limit=101", "?limit=no", "?limit=1&limit=2", "?cursor=broken", "?cursor=%ZZ"} {
		readLedger(tokens["wallet-internal"], ledgerPath+suffix, 400)
	}
	all := readLedger(tokens["wallet-internal"], ledgerPath, 200)
	if len(all.Entries) != 3 || all.NextCursor != "" {
		t.Fatal("unexpected full ledger")
	}
	cursor := ""
	for i := 0; i < 3; i++ {
		page := readLedger(tokens["wallet-internal"], ledgerPath+"?limit=1&cursor="+cursor, 200)
		if len(page.Entries) != 1 || page.Entries[0].ID != all.Entries[i].ID || page.Entries[0].WalletID != walletID {
			t.Fatal("ledger pagination skipped or repeated a movement")
		}
		if i > 0 && page.Entries[0].CreatedAt.Before(all.Entries[i-1].CreatedAt) {
			t.Fatal("ledger order changed")
		}
		cursor = page.NextCursor
		if (i < 2) != (cursor != "") {
			t.Fatal("invalid ledger continuation")
		}
		if i == 0 {
			readLedger(tokens["wallet-internal"], "/wallets/another/ledger?cursor="+cursor, 400)
		}
	}
	expectedFinal, _ := domain.NewMoney(6500, "BRL")
	if all.Entries[2].BalanceAfter != expectedFinal {
		t.Fatal("ledger final balance mismatch")
	}

	// Queries must enforce ownership for both identities and expose persisted outcomes.
	get := func(token, path string, want int) outcome {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, "GET", base+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		if res.StatusCode != want {
			t.Fatalf("query status %d want %d", res.StatusCode, want)
		}
		if want == 401 || want == 403 {
			return outcome{}
		}
		if res.Header.Get("Cache-Control") != "no-store" {
			t.Fatal("query response can be cached")
		}
		var out outcome
		if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	path := "/wagering/transactions/" + original.ID
	get("", path, 401)
	get(tokens["wallet-internal"], path, 403)
	get(tokens["provider-b"], path, 404)
	get(tokens["provider-a"], "/wagering/transactions/nonexistent", 404)
	found := get(tokens["provider-a"], path, 200)
	if found.ID != original.ID || found.Status != original.Status || found.Balance != original.Balance {
		t.Fatal("query did not return persisted original result")
	}
	externalPath := "/providers/provider-a/wagering/transactions/http-bet"
	if get(tokens["provider-a"], externalPath, 200).ID != original.ID {
		t.Fatal("external query identity mismatch")
	}
	get(tokens["provider-b"], externalPath, 403)
	get(tokens["provider-b"], "/providers/provider-b/wagering/transactions/http-bet", 404)
	get(tokens["provider-a"], "/providers/provider-a/wagering/transactions/missing", 404)
	if get(tokens["provider-a"], "/providers/provider-a/wagering/transactions/pending", 200).Status != "PENDING_REFERENCE" {
		t.Fatal("query omitted pending reference")
	}
	if get(tokens["provider-a"], "/providers/provider-a/wagering/transactions/reject", 200).Failure != "INSUFFICIENT_FUNDS" {
		t.Fatal("query omitted rejection code")
	}
	m.Data.Kind = domain.WagerOpening
	send(tokens["provider-a"], "opening-key", 400)
}
