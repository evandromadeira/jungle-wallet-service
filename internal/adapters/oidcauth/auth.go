// Package oidcauth verifies external identities before HTTP handlers access business data.
package oidcauth

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

// Identity contains only verified claims. Its private fields prevent caller-created authority.
type Identity struct{ subject, provider, role string }

// ProviderID returns the authenticated provider, never a value from request data.
func (i Identity) ProviderID() string { return i.provider }

// CanAccessProvider denies cross-provider access, including access by internal wallet services.
func (i Identity) CanAccessProvider(provider string) bool {
	return i.subject != "" && i.role == "provider" && i.provider != "" && i.provider == provider
}

// CanManageWallets restricts wallet operations to the internal service identity.
func (i Identity) CanManageWallets() bool {
	return i.subject != "" && i.role == "internal" && i.provider == ""
}

type identityKey struct{}

// FromContext retrieves an identity installed by this package after verification.
func FromContext(ctx context.Context) (Identity, bool) {
	i, ok := ctx.Value(identityKey{}).(Identity)
	return i, ok
}

// Authenticator owns a cached JWKS verifier; its injected HTTP client must have a timeout.
type Authenticator struct {
	verifier *oidc.IDTokenVerifier
	client   *http.Client
}

// New performs discovery for a trusted configured issuer, not an issuer supplied by a token.
func New(ctx context.Context, issuer, audience string, client *http.Client) (*Authenticator, error) {
	u, e := url.Parse(issuer)
	if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.TrimSpace(audience) == "" || client == nil || client.Timeout <= 0 {
		return nil, errors.New("invalid OIDC configuration")
	}
	provider, e := oidc.NewProvider(oidc.ClientContext(ctx, client), issuer)
	if e != nil {
		return nil, errors.New("OIDC discovery failed")
	}
	return &Authenticator{provider.Verifier(&oidc.Config{ClientID: audience, SupportedSigningAlgs: []string{"RS256"}}), client}, nil
}

// authenticate validates the signature, issuer, audience, expiration and service claim shape.
func (a *Authenticator) authenticate(ctx context.Context, raw string) (Identity, error) {
	token, e := a.verifier.Verify(oidc.ClientContext(ctx, a.client), raw)
	if e != nil {
		return Identity{}, errors.New("invalid access token")
	}
	var c struct {
		Provider  string `json:"providerId"`
		Role      string `json:"serviceRole"`
		Type      string `json:"typ"`
		NotBefore int64  `json:"nbf"`
	}
	if token.Claims(&c) != nil || token.Subject == "" || c.Type != "Bearer" || c.NotBefore > time.Now().Unix() {
		return Identity{}, errors.New("invalid access identity")
	}
	i := Identity{token.Subject, c.Provider, c.Role}
	if c.Role == "provider" && c.Provider != "" && strings.TrimSpace(c.Provider) == c.Provider {
		return i, nil
	}
	if i.CanManageWallets() {
		return i, nil
	}
	return Identity{}, errors.New("invalid service claims")
}

// Require authenticates a single Bearer header before applying the route's explicit policy.
// Policies must also scope database lookups; this middleware alone cannot enforce row ownership.
func (a *Authenticator) Require(allow func(*http.Request, Identity) bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		values := r.Header.Values("Authorization")
		if len(values) != 1 {
			unauthorized(w)
			return
		}
		fields := strings.Fields(values[0])
		if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") || len(fields[1]) > 16384 {
			unauthorized(w)
			return
		}
		i, e := a.authenticate(r.Context(), fields[1])
		if e != nil {
			unauthorized(w)
			return
		}
		if allow == nil || !allow(r, i) {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), identityKey{}, i)))
	})
}

// unauthorized emits a generic challenge without leaking token contents or verifier errors.
func unauthorized(w http.ResponseWriter) {
	w.Header().Set("WWW-Authenticate", `Bearer realm="wallet-api"`)
	http.Error(w, "unauthorized", http.StatusUnauthorized)
}
