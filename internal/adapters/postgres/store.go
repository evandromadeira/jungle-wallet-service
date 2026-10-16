package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store borrows a pool; the bootstrap layer owns its configuration and lifetime.
type Store struct{ pool *pgxpool.Pool }

var _ application.UnitOfWork = (*Store)(nil)

// NewStore rejects an absent pool without creating or migrating a database.
func NewStore(pool *pgxpool.Pool) (*Store, error) {
	if pool == nil {
		return nil, errors.New("postgres pool is required")
	}
	return &Store{pool: pool}, nil
}

// GetWallet reads a committed snapshot without reserving the wallet for a write.
func (s *Store) GetWallet(ctx context.Context, id string) (domain.Wallet, error) {
	return readWallet(ctx, s.pool, id, false)
}

// WithinTransaction commits a successful callback and rolls back errors or panics.
// Cleanup uses a separate bounded context so cancellation does not leave a transaction open.
// The callback is never retried automatically, including after an ambiguous commit failure.
func (s *Store) WithinTransaction(ctx context.Context, fn func(application.Repositories) error) (err error) {
	if fn == nil {
		return errors.New("transaction callback is required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		rollbackErr := tx.Rollback(cleanup)
		if rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("rollback transaction: %w", rollbackErr))
		}
	}()
	if err = fn(application.Repositories{Wallets: &walletRepository{tx: tx}, Wagers: &wagerRepository{tx: tx}, Ledger: &ledgerRepository{tx: tx}, Outbox: &outboxRepository{tx: tx}, Inbox: &inboxRepository{tx: tx}}); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}
