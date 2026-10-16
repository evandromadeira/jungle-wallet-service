package httpapi

import (
	"context"
	"errors"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// queryLedger exposes bounded pages only after internal-service authorization.
func queryLedger(w http.ResponseWriter, r *http.Request, service *application.QueryLedger) {
	params, err := parseLedgerParameters(r)
	if err != nil {
		reject(w, 400, "invalid_request")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	page, err := service.Execute(ctx, r.PathValue("walletId"), params.cursor, params.limit)
	if err != nil {
		switch {
		case errors.Is(err, application.ErrInvalidLedgerQuery):
			reject(w, 400, "invalid_request")
		case errors.Is(err, application.ErrWalletNotFound):
			reject(w, 404, "wallet_not_found")
		default:
			reject(w, 503, "query_unavailable")
		}
		return
	}
	type entry struct {
		ID            string                 `json:"id"`
		WalletID      string                 `json:"walletId"`
		TransactionID string                 `json:"transactionId"`
		Direction     domain.LedgerDirection `json:"direction"`
		Money         domain.Money           `json:"money"`
		BalanceBefore domain.Money           `json:"balanceBefore"`
		BalanceAfter  domain.Money           `json:"balanceAfter"`
		CreatedAt     time.Time              `json:"createdAt"`
	}
	entries := make([]entry, 0, len(page.Entries))
	for _, s := range page.Entries {
		entries = append(entries, entry{s.ID, s.WalletID, s.TransactionID, s.Direction, s.Money, s.BalanceBefore, s.BalanceAfter, s.CreatedAt})
	}
	respond(w, 200, struct {
		Entries    []entry `json:"entries"`
		NextCursor string  `json:"nextCursor,omitempty"`
	}{entries, page.NextCursor})
}

type ledgerParameters struct {
	cursor string
	limit  int
}

// parseLedgerParameters rejects ambiguous or malformed pagination inputs.
func parseLedgerParameters(r *http.Request) (ledgerParameters, error) {
	p := ledgerParameters{limit: 50}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return p, err
	}
	for key, v := range values {
		if len(v) != 1 {
			return p, application.ErrInvalidLedgerQuery
		}
		switch key {
		case "cursor":
			p.cursor = v[0]
		case "limit":
			p.limit, err = strconv.Atoi(v[0])
			if err != nil {
				return p, err
			}
		default:
			return p, application.ErrInvalidLedgerQuery
		}
	}
	return p, nil
}
