package oidcauth

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

type fixtureKeys struct{}

// VerifySignature isolates claim validation; real signature checks run against Keycloak in integration.
func (fixtureKeys) VerifySignature(_ context.Context, raw string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(strings.Split(raw, ".")[1])
}

// TestRejectedClaims ensures invalid identities cannot reach a protected handler.
func TestRejectedClaims(t *testing.T) {
	now := time.Now().Unix()
	a := &Authenticator{oidc.NewVerifier("https://issuer", fixtureKeys{}, &oidc.Config{ClientID: "wallet-api", SupportedSigningAlgs: []string{"RS256"}}), &http.Client{Timeout: time.Second}}
	for _, tc := range []struct {
		name, issuer, audience, role, provider, typ string
		expiry, nbf                                 int64
		status                                      int
	}{
		{"valid", "https://issuer", "wallet-api", "provider", "a", "Bearer", now + 300, 0, 204},
		{"expired", "https://issuer", "wallet-api", "provider", "a", "Bearer", now - 60, 0, 401},
		{"issuer", "https://other", "wallet-api", "provider", "a", "Bearer", now + 300, 0, 401},
		{"audience", "https://issuer", "other", "provider", "a", "Bearer", now + 300, 0, 401},
		{"future", "https://issuer", "wallet-api", "provider", "a", "Bearer", now + 300, now + 120, 401},
		{"mixed authority", "https://issuer", "wallet-api", "internal", "a", "Bearer", now + 300, 0, 401},
		{"missing provider", "https://issuer", "wallet-api", "provider", "", "Bearer", now + 300, 0, 401},
		{"id token", "https://issuer", "wallet-api", "provider", "a", "ID", now + 300, 0, 401},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload := fmt.Sprintf(`{"iss":%q,"aud":%q,"sub":"subject","exp":%d,"nbf":%d,"serviceRole":%q,"providerId":%q,"typ":%q}`, tc.issuer, tc.audience, tc.expiry, tc.nbf, tc.role, tc.provider, tc.typ)
			raw := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"RS256"}`)) + "." + base64.RawURLEncoding.EncodeToString([]byte(payload)) + ".AA"
			h := a.Require(func(_ *http.Request, i Identity) bool { return i.CanAccessProvider("a") }, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }))
			req := httptest.NewRequest("GET", "/", nil)
			req.Header.Set("Authorization", "Bearer "+raw)
			response := httptest.NewRecorder()
			h.ServeHTTP(response, req)
			if response.Code != tc.status {
				t.Fatalf("got %d want %d", response.Code, tc.status)
			}
			req.Header.Add("Authorization", "Bearer "+raw)
			response = httptest.NewRecorder()
			h.ServeHTTP(response, req)
			if response.Code != 401 {
				t.Fatal("duplicate authorization headers accepted")
			}
		})
	}
	if (Identity{}).CanManageWallets() || (Identity{}).CanAccessProvider("") {
		t.Fatal("zero identity grants access")
	}
}
