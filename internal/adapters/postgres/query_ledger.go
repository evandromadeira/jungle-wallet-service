package postgres

import (
	"context"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

// ReadLedger distinguishes missing wallets from empty ledgers and uses an exclusive keyset.
func (s *Store) ReadLedger(ctx context.Context, wallet string, after *application.LedgerPosition, limit int) ([]domain.WalletLedgerState, error) {
	if _, err := s.GetWallet(ctx, wallet); err != nil {
		return nil, err
	}
	predicate := ""
	args := []any{wallet, limit}
	if after != nil {
		predicate = " AND (created_at,id)>($3,$4)"
		args = append(args, after.CreatedAt, after.ID)
	}
	rows, err := s.pool.Query(ctx, "SELECT id,wallet_id,transaction_id,currency,direction,amount_minor,balance_before_minor,balance_after_minor,created_at FROM public.wallet_ledger_entries WHERE wallet_id=$1"+predicate+" ORDER BY created_at,id LIMIT $2", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]domain.WalletLedgerState, 0)
	for rows.Next() {
		var state domain.WalletLedgerState
		var currency string
		var amount, before, balance int64
		if err := rows.Scan(&state.ID, &state.WalletID, &state.TransactionID, &currency, &state.Direction, &amount, &before, &balance, &state.CreatedAt); err != nil {
			return nil, err
		}
		state.Money, err = domain.NewMoney(amount, currency)
		if err != nil {
			return nil, err
		}
		state.BalanceBefore, err = domain.NewMoney(before, currency)
		if err != nil {
			return nil, err
		}
		state.BalanceAfter, err = domain.NewMoney(balance, currency)
		if err != nil {
			return nil, err
		}
		if _, err = domain.RehydrateWalletLedgerEntry(state); err != nil {
			return nil, err
		}
		result = append(result, state)
	}
	return result, rows.Err()
}
