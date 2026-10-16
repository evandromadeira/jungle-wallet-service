package httpapi

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/telemetry"
	"net/http"
	"time"
)

// reconcileWallet serves a read-only audit after internal-service authorization.
func reconcileWallet(w http.ResponseWriter, r *http.Request, service *application.ReconcileWallet) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	correlation := rand.Text()
	w.Header().Set("X-Correlation-ID", correlation)
	result, err := service.Execute(ctx, r.PathValue("walletId"), correlation)
	if err != nil {
		switch {
		case errors.Is(err, application.ErrWalletNotFound):
			reject(w, 404, "wallet_not_found")
		case errors.Is(err, application.ErrInvalidWalletQuery):
			reject(w, 400, "invalid_request")
		default:
			reject(w, 503, "reconciliation_unavailable")
		}
		return
	}
	respond(w, 200, map[string]any{"walletId": result.WalletID, "storedBalance": result.StoredBalance, "calculatedBalance": result.CalculatedBalance, "difference": result.Difference, "consistent": result.Consistent, "checkedEntries": result.CheckedEntries})
}

// reconciliationMetrics exposes a low-cardinality counter without financial identifiers.
func reconciliationMetrics(w http.ResponseWriter, r *http.Request, service *application.ReconcileWallet) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	registry := telemetry.From(ctx)
	gauges, err := registry.Gather(ctx)
	if err != nil {
		reject(w, 503, "metrics_unavailable")
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.Header().Set("Cache-Control", "no-store")
	registry.Write(w)
	telemetry.WriteGauges(w, gauges)
	fmt.Fprintf(w, "# HELP wallet_reconciliation_discrepancies_total Number of divergent reconciliation checks in this process.\n# TYPE wallet_reconciliation_discrepancies_total counter\nwallet_reconciliation_discrepancies_total %d\n", service.Discrepancies())
}
