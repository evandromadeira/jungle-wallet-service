package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type ledgerRepository struct{ tx pgx.Tx }

var _ application.LedgerRepository = (*ledgerRepository)(nil)

// Append inserts one validated entry. Duplicate identity is an error, never a silent replay.
// Database triggers verify linkage to the processed transaction and rollback direction.
func (r *ledgerRepository) Append(ctx context.Context, entry domain.WalletLedgerEntry) error {
	s, err := entry.Snapshot()
	if err != nil {
		return err
	}
	amount, _ := s.Money.Minor()
	currency, _ := s.Money.Currency()
	before, _ := s.BalanceBefore.Minor()
	after, _ := s.BalanceAfter.Minor()
	_, err = r.tx.Exec(ctx, `INSERT INTO public.wallet_ledger_entries
 (id,wallet_id,transaction_id,currency,direction,amount_minor,balance_before_minor,balance_after_minor,created_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, s.ID, s.WalletID, s.TransactionID, currency, s.Direction, amount, before, after, s.CreatedAt)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return application.ErrLedgerAlreadyExists
	}
	if err != nil {
		return fmt.Errorf("append ledger entry: %w", err)
	}
	return nil
}

// GetByTransaction returns an immutable record scoped to its wallet without replaying it.
func (r *ledgerRepository) GetByTransaction(ctx context.Context, walletID, transactionID string) (domain.WalletLedgerEntry, error) {
	var s domain.WalletLedgerState
	var amount, before, after int64
	var currency string
	err := r.tx.QueryRow(ctx, `SELECT id,wallet_id,transaction_id,currency,direction,amount_minor,
 balance_before_minor,balance_after_minor,created_at FROM public.wallet_ledger_entries
 WHERE wallet_id=$1 AND transaction_id=$2`, walletID, transactionID).Scan(&s.ID, &s.WalletID, &s.TransactionID,
		&currency, &s.Direction, &amount, &before, &after, &s.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.WalletLedgerEntry{}, application.ErrLedgerNotFound
	}
	if err != nil {
		return domain.WalletLedgerEntry{}, fmt.Errorf("read ledger entry: %w", err)
	}
	s.Money, err = domain.NewMoney(amount, currency)
	if err != nil {
		return domain.WalletLedgerEntry{}, err
	}
	s.BalanceBefore, err = domain.NewMoney(before, currency)
	if err != nil {
		return domain.WalletLedgerEntry{}, err
	}
	s.BalanceAfter, err = domain.NewMoney(after, currency)
	if err != nil {
		return domain.WalletLedgerEntry{}, err
	}
	return domain.RehydrateWalletLedgerEntry(s)
}
