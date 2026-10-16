//go:build integration

package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"github.com/evandromadeira/jungle-wallet-service/internal/adapters/httpapi"
	"github.com/evandromadeira/jungle-wallet-service/internal/adapters/oidcauth"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

// TestOIDCProvisioning verifies real discovery, client authentication and signed identity claims.
// Tokens and response bodies must never be included in test logs.
func TestOIDCProvisioning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "compose", "port", "keycloak", "8080")
	cmd.Dir = filepath.Join("..", "..")
	address, e := cmd.Output()
	if e != nil {
		t.Fatal("start keycloak with docker compose up -d --wait keycloak")
	}
	issuer := "http://" + strings.TrimSpace(string(address)) + "/realms/jungle-wallet"
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	client := &http.Client{Timeout: 10 * time.Second, Transport: transport}
	ctx = oidc.ClientContext(ctx, client)
	provider, e := oidc.NewProvider(ctx, issuer)
	if e != nil {
		t.Fatal("OIDC discovery failed")
	}
	verifier := provider.Verifier(&oidc.Config{ClientID: "wallet-api", SupportedSigningAlgs: []string{"RS256"}})
	tokenURL := provider.Endpoint().TokenURL
	if !strings.HasPrefix(tokenURL, issuer+"/") {
		t.Fatal("discovery returned unexpected token endpoint")
	}
	request := func(values url.Values) (string, int) {
		t.Helper()
		req, e := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(values.Encode()))
		if e != nil {
			t.Fatal("cannot construct token request")
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		response, e := client.Do(req)
		if e != nil {
			t.Fatal("token request failed")
		}
		defer response.Body.Close()
		var payload struct {
			AccessToken string `json:"access_token"`
			TokenType   string `json:"token_type"`
		}
		if e = json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&payload); e != nil {
			t.Fatal("invalid token response")
		}
		if response.StatusCode == 200 && !strings.EqualFold(payload.TokenType, "bearer") {
			t.Fatal("unexpected token type")
		}
		return payload.AccessToken, response.StatusCode
	}
	auth, e := oidcauth.New(ctx, issuer, "wallet-api", client)
	if e != nil {
		t.Fatal(e)
	}
	checkHTTP := func(raw string, policy func(*http.Request, oidcauth.Identity) bool, want int) {
		t.Helper()
		handler := auth.Require(policy, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, ok := oidcauth.FromContext(r.Context()); !ok {
				t.Error("missing verified identity")
			}
			w.WriteHeader(http.StatusNoContent)
		}))
		req := httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx)
		if raw != "" {
			req.Header.Set("Authorization", "Bearer "+raw)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, req)
		if response.Code != want {
			t.Fatalf("authorization status %d, want %d", response.Code, want)
		}
	}
	providerA := func(_ *http.Request, i oidcauth.Identity) bool { return i.CanAccessProvider("provider-a") }
	internal := func(_ *http.Request, i oidcauth.Identity) bool { return i.CanManageWallets() }
	checkHTTP("", providerA, 401)
	checkHTTP("invalid", providerA, 401)
	// Verify expiration of an unmodified token signed by the real Keycloak instance.
	expiredToken, tokenStatus := request(url.Values{"grant_type": {"client_credentials"}, "client_id": {"provider-expiry-test"}, "client_secret": {"local-provider-expiry-only"}})
	if tokenStatus != 200 {
		t.Fatalf("short-lived token status: %d", tokenStatus)
	}
	verified, err := verifier.Verify(ctx, expiredToken)
	if err != nil {
		t.Fatal("fresh short-lived token rejected")
	}
	if remaining := time.Until(verified.Expiry); remaining <= 0 || remaining > 6*time.Second {
		t.Fatal("unexpected short-lived token lifetime")
	}
	checkHTTP(expiredToken, providerA, 204)
	timer := time.NewTimer(time.Until(verified.Expiry.Add(time.Second)))
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		t.Fatal("expiration wait timed out")
	}
	checkHTTP(expiredToken, providerA, 401)
	store, _, adminDSN := restrictedStore(t)
	opening, e := application.NewOpenWallet(store, func() (string, error) { return rand.Text(), nil }, time.Now)
	if e != nil {
		t.Fatal(e)
	}
	processor, e := application.NewProcessWager(store, func() (string, error) { return rand.Text(), nil }, time.Now)
	if e != nil {
		t.Fatal(e)
	}
	queries, e := application.NewQueryWager(store)
	if e != nil {
		t.Fatal(e)
	}
	walletQueries, e := application.NewQueryWallet(store)
	if e != nil {
		t.Fatal(e)
	}
	ledgerQueries, e := application.NewQueryLedger(store)
	if e != nil {
		t.Fatal(e)
	}
	var reconciliationLogs bytes.Buffer
	reconciliation, e := application.NewReconcileWallet(store, slog.New(slog.NewJSONHandler(&reconciliationLogs, nil)))
	if e != nil {
		t.Fatal(e)
	}
	routes, e := httpapi.New(auth, opening, processor, queries, walletQueries, ledgerQueries, reconciliation)
	if e != nil {
		t.Fatal(e)
	}
	server := httptest.NewServer(routes)
	defer server.Close()
	post := func(token, body, contentType string, want int) string {
		t.Helper()
		req, e := http.NewRequestWithContext(ctx, "POST", server.URL+"/wallets", strings.NewReader(body))
		if e != nil {
			t.Fatal(e)
		}
		req.Header.Set("Content-Type", contentType)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		res, e := client.Do(req)
		if e != nil {
			t.Fatal("wallet HTTP request failed")
		}
		defer res.Body.Close()
		if res.StatusCode != want {
			t.Fatalf("wallet HTTP status %d, want %d", res.StatusCode, want)
		}
		if want != 201 {
			return ""
		}
		var got struct {
			ID       string       `json:"id"`
			PlayerID string       `json:"playerId"`
			Balance  domain.Money `json:"balance"`
			Version  int64        `json:"version"`
		}
		if e = json.NewDecoder(res.Body).Decode(&got); e != nil {
			t.Fatal(e)
		}
		wallet, e := store.GetWallet(ctx, got.ID)
		if e != nil {
			t.Fatal(e)
		}
		state, _ := wallet.Snapshot()
		if got.PlayerID != state.PlayerID || got.Balance != state.Balance || got.Version != 1 || res.Header.Get("X-Correlation-ID") == "" {
			t.Fatal("response differs from committed wallet")
		}
		return got.ID
	}
	validBody := `{"playerId":"http-player","initialBalance":{"amount":"100.00","currency":"BRL"}}`
	post("", validBody, "application/json", 401)
	tokens := map[string]string{}
	subjects := map[string]bool{}
	for _, id := range []string{"provider-a", "provider-b", "wallet-internal"} {
		values := url.Values{"grant_type": {"client_credentials"}, "client_id": {id}, "client_secret": {"local-" + id + "-only"}, "providerId": {"attacker"}, "serviceRole": {"internal"}}
		raw, status := request(values)
		if status != 200 || raw == "" {
			t.Fatalf("client %s did not receive a token (HTTP %d)", id, status)
		}
		tokens[id] = raw
		providerStatus, internalStatus := 403, 403
		if id == "provider-a" {
			providerStatus = 204
		}
		if id == "wallet-internal" {
			internalStatus = 204
		}
		checkHTTP(raw, providerA, providerStatus)
		checkHTTP(raw, internal, internalStatus)
		if id != "wallet-internal" {
			post(raw, validBody, "application/json", 403)
		} else {
			post(raw, validBody, "application/json", 201)
			post(raw, validBody, "application/json", 409)
			post(raw, `{"playerId":"http-zero","initialBalance":{"amount":"0.00","currency":"BRL"}}`, "application/json", 201)
			post(raw, validBody, "text/plain", 415)
			for _, body := range []string{`{}`, `{"playerId":"p","initialBalance":{"amount":"-1.00","currency":"BRL"}}`, validBody + ` {}`, `{"playerId":"p","initialBalance":{"amount":"1.00","currency":"BRL","extra":true}}`} {
				post(raw, body, "application/json", 400)
			}
			post(raw, `{"playerId":"`+strings.Repeat("x", 17000)+`"}`, "application/json", 413)
		}
		token, e := verifier.Verify(ctx, raw)
		if e != nil {
			t.Fatal("issued token failed signature/issuer/audience/expiry validation")
		}
		var claims struct {
			ProviderID  string `json:"providerId"`
			ServiceRole string `json:"serviceRole"`
			ClientID    string `json:"azp"`
		}
		if e = token.Claims(&claims); e != nil {
			t.Fatal("cannot decode verified claims")
		}
		wantRole, wantProvider := "provider", id
		if id == "wallet-internal" {
			wantRole = "internal"
			wantProvider = ""
		}
		if claims.ProviderID != wantProvider || claims.ServiceRole != wantRole || claims.ClientID != id {
			t.Fatal("identity claims differ from provisioned client")
		}
		if token.Subject == "" || subjects[token.Subject] {
			t.Fatal("service accounts must have distinct subjects")
		}
		subjects[token.Subject] = true
		if token.Expiry.Before(time.Now()) || token.Expiry.After(time.Now().Add(6*time.Minute)) {
			t.Fatal("unexpected token expiration")
		}
		if _, e = provider.Verifier(&oidc.Config{ClientID: "different-api"}).Verify(ctx, raw); e == nil {
			t.Fatal("wrong audience accepted")
		}
		parts := strings.Split(raw, ".")
		if len(parts) != 3 {
			t.Fatal("invalid JWT structure")
		}
		if parts[2][0] == 'A' {
			parts[2] = "B" + parts[2][1:]
		} else {
			parts[2] = "A" + parts[2][1:]
		}
		if _, e = verifier.Verify(ctx, strings.Join(parts, ".")); e == nil {
			t.Fatal("tampered signature accepted")
		}
		values.Set("client_secret", "incorrect")
		if raw, status = request(values); status != 401 || raw != "" {
			t.Fatal("incorrect client secret accepted")
		}
	}
	testHTTPWagers(t, ctx, client, server.URL, tokens, processor, post(tokens["wallet-internal"], `{"playerId":"http-wager","initialBalance":{"amount":"100.00","currency":"BRL"}}`, "application/json", 201))
	testHTTPReconciliation(t, ctx, client, server.URL, tokens, adminDSN, &reconciliationLogs)
	for _, values := range []url.Values{
		{"grant_type": {"client_credentials"}, "client_id": {"provider-a"}},
		{"grant_type": {"password"}, "client_id": {"provider-a"}, "client_secret": {"local-provider-a-only"}, "username": {"someone"}, "password": {"irrelevant"}},
	} {
		if raw, status := request(values); status < 400 || raw != "" {
			t.Fatal("unconfigured authentication flow accepted")
		}
	}
}
