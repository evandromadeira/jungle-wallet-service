package application

import (
	"context"
	"errors"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
	"github.com/evandromadeira/jungle-wallet-service/internal/telemetry"
	"time"
)

// observeWager is deferred until commit outcome is known; raw errors and payloads are omitted.
func observeWager(ctx context.Context, operation string, start time.Time, result WagerResult, err error, correlation, message, provider, wallet string) {
	r := telemetry.From(ctx)
	status := "ERROR"
	transaction := ""
	if err == nil {
		if state, e := result.Transaction.Snapshot(); e == nil {
			status = string(state.Status)
			transaction = state.ID
			wallet = state.Request.WalletID

			if result.IdempotentReplay {
				r.Count("wallet_duplicates_total")
			}
		}
	}
	if errors.Is(err, ErrWalletConflict) || errors.Is(err, ErrWagerConflict) {
		r.Count("wallet_concurrency_conflicts_total")
	}
	if errors.Is(err, domain.ErrWagerPayloadConflict) {
		r.Count("wallet_idempotency_conflicts_total")
	}
	r.Observe(ctx, operation, status, start, "correlationId", correlation, "messageId", message, "providerId", provider, "walletId", wallet, "transactionId", transaction)
}
