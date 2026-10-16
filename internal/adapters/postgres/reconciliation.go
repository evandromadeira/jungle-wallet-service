package postgres

import (
	"context"
	"errors"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
	"github.com/jackc/pgx/v5"
)

// ReadReconciliation uses one SELECT snapshot for both wallet and ledger.
// Numeric SUM avoids intermediate bigint overflow; an out-of-range final sum fails.
func (s *Store) ReadReconciliation(ctx context.Context, id string) (application.Reconciliation, error) {
	result := application.Reconciliation{WalletID: id}
	var currency string
	var stored, calculated int64
	err := s.pool.QueryRow(ctx, "SELECT w.currency,w.balance_minor,COALESCE(SUM(CASE WHEN l.direction='CREDIT' THEN l.amount_minor::numeric ELSE -l.amount_minor::numeric END),0)::bigint,COUNT(l.id) FROM public.wallets w LEFT JOIN public.wallet_ledger_entries l ON l.wallet_id=w.id WHERE w.id=$1 GROUP BY w.id,w.currency,w.balance_minor", id).Scan(&currency, &stored, &calculated, &result.CheckedEntries)
	if errors.Is(err, pgx.ErrNoRows) {
		return result, application.ErrWalletNotFound
	}
	if err != nil {
		return result, err
	}
	result.StoredBalance, err = domain.NewMoney(stored, currency)
	if err != nil {
		return result, err
	}
	result.CalculatedBalance, err = domain.NewMoney(calculated, currency)
	return result, err
}
