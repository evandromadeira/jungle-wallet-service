package httpapi

import (
	"context"
	"crypto/rand"
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

// wagerRequest keeps transport names separate from domain and excludes the idempotency key.
type wagerRequest struct {
	ProviderID            string           `json:"providerId"`
	ExternalTransactionID string           `json:"externalTransactionId"`
	PlayerID              string           `json:"playerId"`
	WalletID              string           `json:"walletId"`
	RoundID               string           `json:"roundId"`
	GameID                string           `json:"gameId"`
	Kind                  domain.WagerKind `json:"kind"`
	Money                 struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	} `json:"money"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}

// processWager accepts only authenticated provider operations and preserves the caller's key.
func processWager(w http.ResponseWriter, r *http.Request, service *application.ProcessWager) {
	identity, ok := oidcauth.FromContext(r.Context())
	if !ok {
		reject(w, 401, "unauthorized")
		return
	}
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 || keys[0] == "" || strings.TrimSpace(keys[0]) != keys[0] || len(keys[0]) > 256 {
		reject(w, 400, "invalid_idempotency_key")
		return
	}
	media, _, e := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if e != nil || media != "application/json" {
		reject(w, 415, "unsupported_media_type")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16<<10)
	defer r.Body.Close()
	var input wagerRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	e = decoder.Decode(&input)
	if e == nil {
		if trailing := decoder.Decode(new(any)); trailing != io.EOF {
			e = trailing
			if e == nil {
				e = errors.New("multiple JSON values")
			}
		}
	}
	if e != nil {
		var limit *http.MaxBytesError
		if errors.As(e, &limit) {
			reject(w, 413, "body_too_large")
		} else {
			reject(w, 400, "invalid_request")
		}
		return
	}
	if !identity.CanAccessProvider(input.ProviderID) {
		reject(w, 403, "forbidden")
		return
	}
	money, e := domain.ParseMoney(input.Money.Amount, input.Money.Currency)
	if e != nil {
		reject(w, 400, "invalid_request")
		return
	}
	correlation := rand.Text()
	w.Header().Set("X-Correlation-ID", correlation)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	result, e := service.Execute(ctx, application.WagerCommand{AuthorizedProviderID: identity.ProviderID(), CorrelationID: correlation, Request: domain.WagerRequest{
		ProviderID: input.ProviderID, ExternalTransactionID: input.ExternalTransactionID, IdempotencyKey: keys[0], WalletID: input.WalletID, PlayerID: input.PlayerID, RoundID: input.RoundID, GameID: input.GameID, Kind: input.Kind, Money: money, ReferenceExternalTransactionID: input.ReferenceExternalTransactionID}})
	if e != nil {
		switch {
		case errors.Is(e, domain.ErrWagerPayloadConflict):
			reject(w, 409, "idempotency_conflict")
		case errors.Is(e, application.ErrProviderMismatch):
			reject(w, 403, "forbidden")
		case errors.Is(e, application.ErrWalletNotFound):
			reject(w, 404, "wallet_not_found")
		case errors.Is(e, application.ErrInvalidWagerCommand), errors.Is(e, application.ErrWagerWalletMismatch), errors.Is(e, domain.ErrInvalidWager), errors.Is(e, domain.ErrInvalidWagerKind), errors.Is(e, domain.ErrInvalidAmount), errors.Is(e, domain.ErrWagerReferenceMismatch):
			reject(w, 400, "invalid_request")
		default:
			reject(w, 503, "processing_unavailable")
		}
		return
	}
	state, e := result.Transaction.Snapshot()
	if e != nil {
		reject(w, 500, "internal_error")
		return
	}
	status := http.StatusOK
	switch state.Status {
	case domain.WagerPending, domain.WagerPendingReference:
		status = http.StatusAccepted
	case domain.WagerRejected:
		status = http.StatusUnprocessableEntity
	case domain.WagerFailed:
		status = http.StatusInternalServerError
	}
	var balance *domain.Money
	if state.HasResult {
		balance = &state.ResultBalance
	}
	respond(w, status, struct {
		TransactionID    string             `json:"transactionId"`
		Status           domain.WagerStatus `json:"status"`
		Balance          *domain.Money      `json:"balance,omitempty"`
		FailureCode      domain.FailureCode `json:"failureCode,omitempty"`
		IdempotentReplay bool               `json:"idempotentReplay"`
	}{state.ID, state.Status, balance, state.FailureCode, result.IdempotentReplay})
}
