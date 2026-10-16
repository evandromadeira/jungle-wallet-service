package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type walletRepository struct{ tx pgx.Tx }

var _ application.WalletRepository = (*walletRepository)(nil)

type rowQuerier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// readWallet rehydrates the persisted state without applying financial effects.
func readWallet(ctx context.Context, q rowQuerier, id string, lock bool) (domain.Wallet, error) {
	sql := `SELECT id,player_id,currency,balance_minor,version,created_at,updated_at FROM public.wallets WHERE id=$1`
	if lock {
		sql += " FOR UPDATE"
	}
	var state domain.WalletState
	var currency string
	var minor int64
	err := q.QueryRow(ctx, sql, id).Scan(&state.ID, &state.PlayerID, &currency, &minor, &state.Version, &state.CreatedAt, &state.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Wallet{}, application.ErrWalletNotFound
	}
	if err != nil {
		return domain.Wallet{}, fmt.Errorf("read wallet: %w", err)
	}
	state.Balance, err = domain.NewMoney(minor, currency)
	if err != nil {
		return domain.Wallet{}, err
	}
	return domain.RehydrateWallet(state)
}

// Get reads through the current transaction without acquiring a row lock.
func (r *walletRepository) Get(ctx context.Context, id string) (domain.Wallet, error) {
	return readWallet(ctx, r.tx, id, false)
}

// GetForUpdate holds the wallet row lock until transaction completion.
func (r *walletRepository) GetForUpdate(ctx context.Context, id string) (domain.Wallet, error) {
	return readWallet(ctx, r.tx, id, true)
}

// Create persists a new wallet. Positive openings still require their transaction,
// ledger and outbox rows in the same unit of work, to be added by the use case.
func (r *walletRepository) Create(ctx context.Context, w domain.Wallet) error {
	s, err := w.Snapshot()
	if err != nil {
		return err
	}
	if s.Version != 1 || !s.CreatedAt.Equal(s.UpdatedAt) {
		return application.ErrInvalidWalletUpdate
	}
	minor, _ := s.Balance.Minor()
	currency, _ := s.Balance.Currency()
	_, err = r.tx.Exec(ctx, `INSERT INTO public.wallets(id,player_id,currency,balance_minor,version,created_at,updated_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7)`, s.ID, s.PlayerID, currency, minor, s.Version, s.CreatedAt, s.UpdatedAt)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return application.ErrWalletAlreadyExists
	}
	if err != nil {
		return fmt.Errorf("create wallet: %w", err)
	}
	return nil
}

// Update persists one balance change only if the original snapshot still matches.
// Zero movements must skip Update; identity and creation time cannot be changed.
func (r *walletRepository) Update(ctx context.Context, before, after domain.Wallet) error {
	b, err := before.Snapshot()
	if err != nil {
		return err
	}
	a, err := after.Snapshot()
	if err != nil {
		return err
	}
	bc, _ := b.Balance.Currency()
	ac, _ := a.Balance.Currency()
	bm, _ := b.Balance.Minor()
	am, _ := a.Balance.Minor()
	if a.ID != b.ID || a.PlayerID != b.PlayerID || ac != bc || !a.CreatedAt.Equal(b.CreatedAt) ||
		b.Version == math.MaxInt64 || a.Version != b.Version+1 || a.UpdatedAt.Before(b.UpdatedAt) || am == bm {
		return application.ErrInvalidWalletUpdate
	}
	tag, err := r.tx.Exec(ctx, `UPDATE public.wallets SET balance_minor=$1,version=$2,updated_at=$3
 WHERE id=$4 AND version=$5 AND balance_minor=$6 AND player_id=$7 AND currency=$8
 AND created_at=$9 AND updated_at=$10`, am, a.Version, a.UpdatedAt, a.ID, b.Version, bm, b.PlayerID, bc, b.CreatedAt, b.UpdatedAt)
	if err != nil {
		return fmt.Errorf("update wallet: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return application.ErrWalletConflict
	}
	return nil
}
