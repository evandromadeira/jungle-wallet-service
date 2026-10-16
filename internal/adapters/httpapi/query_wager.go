package httpapi

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/adapters/oidcauth"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

// queryWager returns the current persisted state, including pending and rejected outcomes.
func queryWager(w http.ResponseWriter, r *http.Request, service *application.QueryWager, external bool) {
	identity, ok := oidcauth.FromContext(r.Context())
	if !ok {
		reject(w, 401, "unauthorized")
		return
	}
	id := r.PathValue("transactionId")
	if external {
		if !identity.CanAccessProvider(r.PathValue("providerId")) {
			reject(w, 403, "forbidden")
			return
		}
		id = r.PathValue("externalTransactionId")
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	state, err := service.Execute(ctx, identity.ProviderID(), id, external)
	if err != nil {
		switch {
		case errors.Is(err, application.ErrWagerNotFound):
			reject(w, 404, "transaction_not_found")
		case errors.Is(err, application.ErrInvalidWagerCommand):
			reject(w, 400, "invalid_request")
		default:
			reject(w, 503, "query_unavailable")
		}
		return
	}
	var balance *domain.Money
	if state.HasResult {
		balance = &state.ResultBalance
	}
	respond(w, 200, struct {
		TransactionID         string             `json:"transactionId"`
		ProviderID            string             `json:"providerId"`
		ExternalTransactionID string             `json:"externalTransactionId"`
		Status                domain.WagerStatus `json:"status"`
		Balance               *domain.Money      `json:"balance,omitempty"`
		FailureCode           domain.FailureCode `json:"failureCode,omitempty"`
		CreatedAt             time.Time          `json:"createdAt"`
		UpdatedAt             time.Time          `json:"updatedAt"`
	}{state.ID, state.Request.ProviderID, state.Request.ExternalTransactionID, state.Status, balance, state.FailureCode, state.CreatedAt, state.UpdatedAt})
}
