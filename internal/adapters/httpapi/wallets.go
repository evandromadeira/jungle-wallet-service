// Package httpapi exposes authenticated financial operations over HTTP.
package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/adapters/oidcauth"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

// New constructs the authenticated routes without starting a listener or owning dependencies.
func New(auth *oidcauth.Authenticator, opening *application.OpenWallet, wagers *application.ProcessWager, queries *application.QueryWager, walletQueries *application.QueryWallet, ledgerQueries *application.QueryLedger, reconciliation *application.ReconcileWallet) (http.Handler, error) {
	if auth == nil || opening == nil || wagers == nil || queries == nil || walletQueries == nil || ledgerQueries == nil || reconciliation == nil {
		return nil, errors.New("HTTP authentication, opening, wagering, and query services are required")
	}
	mux := http.NewServeMux()
	mux.Handle("POST /wallets", auth.Require(func(_ *http.Request, i oidcauth.Identity) bool { return i.CanManageWallets() }, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { openWallet(w, r, opening) })))
	mux.Handle("POST /wagering/transactions", auth.Require(func(_ *http.Request, i oidcauth.Identity) bool { return i.CanAccessProvider(i.ProviderID()) }, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { processWager(w, r, wagers) })))
	mux.Handle("GET /wagering/transactions/{transactionId}", auth.Require(func(_ *http.Request, i oidcauth.Identity) bool { return i.CanAccessProvider(i.ProviderID()) }, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { queryWager(w, r, queries, false) })))
	mux.Handle("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", auth.Require(func(_ *http.Request, i oidcauth.Identity) bool { return i.CanAccessProvider(i.ProviderID()) }, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { queryWager(w, r, queries, true) })))
	mux.Handle("GET /wallets/{walletId}", auth.Require(func(_ *http.Request, i oidcauth.Identity) bool { return i.CanManageWallets() }, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { queryWallet(w, r, walletQueries) })))
	mux.Handle("GET /wallets/{walletId}/ledger", auth.Require(func(_ *http.Request, i oidcauth.Identity) bool { return i.CanManageWallets() }, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { queryLedger(w, r, ledgerQueries) })))
	mux.Handle("POST /wallets/{walletId}/reconciliation", auth.Require(func(_ *http.Request, i oidcauth.Identity) bool { return i.CanManageWallets() }, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reconcileWallet(w, r, reconciliation) })))
	mux.Handle("GET /metrics", auth.Require(func(_ *http.Request, i oidcauth.Identity) bool { return i.CanManageWallets() }, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { reconciliationMetrics(w, r, reconciliation) })))
	return mux, nil
}

// respond writes a JSON response without exposing underlying persistence errors.
func respond(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

// reject emits stable machine-readable errors, never database details or financial input.
func reject(w http.ResponseWriter, status int, code string) {
	respond(w, status, struct {
		Error string `json:"error"`
	}{code})
}

// openWallet strictly decodes a bounded body and returns 201 only after the application commit.
func openWallet(w http.ResponseWriter, r *http.Request, opening *application.OpenWallet) {
	media, _, e := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if e != nil || media != "application/json" {
		reject(w, 415, "unsupported_media_type")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	defer r.Body.Close()
	var input struct {
		PlayerID       string `json:"playerId"`
		InitialBalance struct {
			Amount   string `json:"amount"`
			Currency string `json:"currency"`
		} `json:"initialBalance"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	e = decoder.Decode(&input)
	if e == nil {
		if trailing := decoder.Decode(new(any)); trailing != io.EOF {
			if trailing == nil {
				e = errors.New("multiple JSON values")
			} else {
				e = trailing
			}
		}
	}
	if e != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(e, &tooLarge) {
			reject(w, 413, "body_too_large")
		} else {
			reject(w, 400, "invalid_request")
		}
		return
	}
	balance, e := domain.ParseMoney(input.InitialBalance.Amount, input.InitialBalance.Currency)
	if e != nil || input.PlayerID == "" || strings.TrimSpace(input.PlayerID) != input.PlayerID {
		reject(w, 400, "invalid_request")
		return
	}
	// Generate trusted correlation metadata; callers cannot inject arbitrary log identifiers.
	var bytes [16]byte
	if _, e = rand.Read(bytes[:]); e != nil {
		reject(w, 500, "internal_error")
		return
	}
	correlation := hex.EncodeToString(bytes[:])
	w.Header().Set("X-Correlation-ID", correlation)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	wallet, e := opening.Execute(ctx, application.OpenWalletCommand{PlayerID: input.PlayerID, InitialBalance: balance, CorrelationID: correlation})
	if errors.Is(e, application.ErrWalletAlreadyExists) {
		reject(w, 409, "wallet_already_exists")
		return
	}
	if e != nil {
		reject(w, 500, "internal_error")
		return
	}
	state, e := wallet.Snapshot()
	if e != nil {
		reject(w, 500, "internal_error")
		return
	}
	respond(w, 201, struct {
		ID       string       `json:"id"`
		PlayerID string       `json:"playerId"`
		Balance  domain.Money `json:"balance"`
		Version  int64        `json:"version"`
	}{state.ID, state.PlayerID, state.Balance, state.Version})
}
