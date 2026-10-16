package httpapi

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

// queryWallet returns current balance and version after internal-service authorization.
func queryWallet(w http.ResponseWriter, r *http.Request, service *application.QueryWallet) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	state, err := service.Execute(ctx, r.PathValue("walletId"))
	if err != nil {
		switch {
		case errors.Is(err, application.ErrWalletNotFound):
			reject(w, 404, "wallet_not_found")
		case errors.Is(err, application.ErrInvalidWalletQuery):
			reject(w, 400, "invalid_request")
		default:
			reject(w, 503, "query_unavailable")
		}
		return
	}
	respond(w, 200, struct {
		ID        string       `json:"id"`
		PlayerID  string       `json:"playerId"`
		Balance   domain.Money `json:"balance"`
		Version   int64        `json:"version"`
		CreatedAt time.Time    `json:"createdAt"`
		UpdatedAt time.Time    `json:"updatedAt"`
	}{state.ID, state.PlayerID, state.Balance, state.Version, state.CreatedAt, state.UpdatedAt})
}
