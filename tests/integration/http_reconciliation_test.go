//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// testHTTPReconciliation corrupts only an isolated fixture and verifies audit never repairs it.
func testHTTPReconciliation(t *testing.T, ctx context.Context, client *http.Client, base string, tokens map[string]string, adminDSN string, logs *bytes.Buffer) {
	t.Helper()
	request := func(method, path, body, token string, want int) []byte {
		t.Helper()
		req, err := http.NewRequestWithContext(ctx, method, base+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		data, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != want {
			t.Fatalf("%s %s status %d want %d", method, path, res.StatusCode, want)
		}
		return data
	}
	internal := tokens["wallet-internal"]
	create := func(player, amount string) string {
		t.Helper()
		data := request("POST", "/wallets", `{"playerId":"`+player+`","initialBalance":{"amount":"`+amount+`","currency":"BRL"}}`, internal, 201)
		var out struct{ ID string }
		if json.Unmarshal(data, &out) != nil || out.ID == "" {
			t.Fatal("invalid opening")
		}
		return out.ID
	}
	check := func(id, stored, calculated, difference string, consistent bool, count int64) {
		t.Helper()
		data := request("POST", "/wallets/"+id+"/reconciliation", "", internal, 200)
		// Difference is signed output; external-input Money decoding intentionally rejects negatives.
		type money struct{ Amount, Currency string }
		var out struct {
			WalletID                                     string
			StoredBalance, CalculatedBalance, Difference money
			Consistent                                   bool
			CheckedEntries                               int64
		}
		if json.Unmarshal(data, &out) != nil {
			t.Fatal("invalid audit JSON")
		}
		if out.WalletID != id || out.StoredBalance.Amount != stored || out.CalculatedBalance.Amount != calculated || out.Difference.Amount != difference || out.Difference.Currency != "BRL" || out.Consistent != consistent || out.CheckedEntries != count {
			t.Fatal("audit result differs from fixture")
		}
	}
	metrics := func(want string) {
		t.Helper()
		data := request("GET", "/metrics", "", internal, 200)
		if !strings.Contains(string(data), "# TYPE wallet_reconciliation_discrepancies_total counter\n") || !strings.Contains(string(data), "\nwallet_reconciliation_discrepancies_total "+want+"\n") {
			t.Fatal("invalid discrepancy metric")
		}
	}
	request("GET", "/metrics", "", "", 401)
	request("GET", "/metrics", "", tokens["provider-a"], 403)
	metrics("0")
	empty := create("reconcile-empty", "0.00")
	check(empty, "0.00", "0.00", "0.00", true, 0)
	funded := create("reconcile-corrupt", "100.00")
	check(funded, "100.00", "100.00", "0.00", true, 1)
	metrics("0")
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	var beforeVersion int64
	if err := admin.QueryRow(ctx, "SELECT version FROM public.wallets WHERE id=$1", funded).Scan(&beforeVersion); err != nil {
		t.Fatal(err)
	}
	// Deliberate test-only inconsistency: no ledger update, trigger bypass or development data.
	tag, err := admin.Exec(ctx, "UPDATE public.wallets SET balance_minor=9000 WHERE id=$1", funded)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatal("cannot prepare isolated discrepancy")
	}
	check(funded, "90.00", "100.00", "-10.00", false, 1)
	metrics("1")
	check(funded, "90.00", "100.00", "-10.00", false, 1)
	metrics("2")
	var balance, version, entries int64
	if err := admin.QueryRow(ctx, "SELECT balance_minor,version,(SELECT count(*) FROM public.wallet_ledger_entries WHERE wallet_id=$1) FROM public.wallets WHERE id=$1", funded).Scan(&balance, &version, &entries); err != nil {
		t.Fatal(err)
	}
	if balance != 9000 || version != beforeVersion || entries != 1 {
		t.Fatal("audit modified persisted state")
	}
	decoder := json.NewDecoder(bytes.NewReader(logs.Bytes()))
	warnings := 0
	for {
		var record map[string]any
		err := decoder.Decode(&record)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if record["msg"] == "wallet reconciliation discrepancy" {
			warnings++
			if record["walletId"] != funded || record["correlationId"] == "" || record["correlationId"] == nil {
				t.Fatal("missing audit identifiers")
			}
		}
	}
	if warnings != 2 {
		t.Fatal("missing discrepancy logs")
	}
}
