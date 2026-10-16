package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
	"github.com/jackc/pgx/v5"
)

type wagerRepository struct{ tx pgx.Tx }

var _ application.WagerRepository = (*wagerRepository)(nil)

const wagerColumns = `id,origin,wallet_id,player_id,currency,kind,amount_minor,
 COALESCE(provider_id,''),COALESCE(external_transaction_id,''),COALESCE(idempotency_key,''),
 COALESCE(payload_hash,''),COALESCE(round_id,''),COALESCE(game_id,''),
 COALESCE(reference_external_transaction_id,''),COALESCE(reference_transaction_id,''),
 status,COALESCE(failure_code,''),result_balance_minor,created_at,updated_at`

// read rehydrates all business and result fields; SQL NULL preserves absent results.
func (r *wagerRepository) read(ctx context.Context, predicate string, lock bool, args ...any) (domain.WagerTransaction, error) {
	return readWager(ctx, r.tx, predicate, lock, args...)
}

// readWager shares snapshot decoding between transactional writes and committed queries.
func readWager(ctx context.Context, query interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, predicate string, lock bool, args ...any) (domain.WagerTransaction, error) {
	sql := `SELECT ` + wagerColumns + ` FROM public.wager_transactions WHERE ` + predicate
	if lock {
		sql += " FOR UPDATE"
	}
	var s domain.WagerState
	var currency string
	var amount int64
	var result *int64
	err := query.QueryRow(ctx, sql, args...).Scan(&s.ID, &s.Origin, &s.Request.WalletID, &s.Request.PlayerID,
		&currency, &s.Request.Kind, &amount, &s.Request.ProviderID, &s.Request.ExternalTransactionID,
		&s.Request.IdempotencyKey, &s.PayloadHash, &s.Request.RoundID, &s.Request.GameID,
		&s.Request.ReferenceExternalTransactionID, &s.ReferenceTransactionID, &s.Status, &s.FailureCode,
		&result, &s.CreatedAt, &s.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.WagerTransaction{}, application.ErrWagerNotFound
	}
	if err != nil {
		return domain.WagerTransaction{}, fmt.Errorf("read wager: %w", err)
	}
	s.Request.Money, err = domain.NewMoney(amount, currency)
	if err != nil {
		return domain.WagerTransaction{}, err
	}
	if result != nil {
		s.HasResult = true
		s.ResultBalance, err = domain.NewMoney(*result, currency)
		if err != nil {
			return domain.WagerTransaction{}, err
		}
	}
	return domain.RehydrateWagerTransaction(s)
}

// Get reads an internal transaction identity, including INTERNAL openings.
func (r *wagerRepository) Get(ctx context.Context, id string) (domain.WagerTransaction, error) {
	return r.read(ctx, "id=$1", false, id)
}

// GetByExternalID scopes a business identity to its authorized provider.
func (r *wagerRepository) GetByExternalID(ctx context.Context, provider, id string) (domain.WagerTransaction, error) {
	return r.read(ctx, "provider_id=$1 AND external_transaction_id=$2", false, provider, id)
}

// GetByIdempotencyKey reads the originally supplied key without replacing it.
func (r *wagerRepository) GetByIdempotencyKey(ctx context.Context, provider, key string) (domain.WagerTransaction, error) {
	return r.read(ctx, "provider_id=$1 AND idempotency_key=$2", false, provider, key)
}

// nullableText maps absent domain metadata to SQL NULL, including internal openings.
func nullableText(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// wagerResult preserves a valid zero balance separately from no result.
func wagerResult(s domain.WagerState) any {
	if !s.HasResult {
		return nil
	}
	minor, _ := s.ResultBalance.Minor()
	return minor
}

// Create persists a validated snapshot without overwriting an existing identity.
// A false result is not proof of replay: key and payload checks belong to the use case.
func (r *wagerRepository) Create(ctx context.Context, w domain.WagerTransaction) (bool, error) {
	s, err := w.Snapshot()
	if err != nil {
		return false, err
	}
	amount, _ := s.Request.Money.Minor()
	currency, _ := s.Request.Money.Currency()
	q := s.Request
	tag, err := r.tx.Exec(ctx, `INSERT INTO public.wager_transactions
 (id,origin,wallet_id,player_id,currency,kind,amount_minor,provider_id,external_transaction_id,
 idempotency_key,payload_hash,round_id,game_id,reference_external_transaction_id,reference_transaction_id,
 status,failure_code,result_balance_minor,created_at,updated_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
 ON CONFLICT DO NOTHING`, s.ID, s.Origin, q.WalletID, q.PlayerID, currency, q.Kind, amount,
		nullableText(q.ProviderID), nullableText(q.ExternalTransactionID), nullableText(q.IdempotencyKey),
		nullableText(s.PayloadHash), nullableText(q.RoundID), nullableText(q.GameID), nullableText(q.ReferenceExternalTransactionID),
		nullableText(s.ReferenceTransactionID), s.Status, nullableText(string(s.FailureCode)), wagerResult(s), s.CreatedAt, s.UpdatedAt)
	if err != nil {
		return false, fmt.Errorf("create wager: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// persistedWagerState matches PostgreSQL timestamp precision for optimistic comparison.
func persistedWagerState(s domain.WagerState) domain.WagerState {
	s.CreatedAt = s.CreatedAt.UTC().Truncate(time.Microsecond)
	s.UpdatedAt = s.UpdatedAt.UTC().Truncate(time.Microsecond)
	return s
}

// Update locks and compares the original snapshot before changing mutable state.
// Retry scheduling columns are intentionally untouched. Lock wallets before wagers
// in financial use cases so all workers use the same lock order.
func (r *wagerRepository) Update(ctx context.Context, before, after domain.WagerTransaction) error {
	b, err := before.Snapshot()
	if err != nil {
		return err
	}
	a, err := after.Snapshot()
	if err != nil {
		return err
	}
	if b.Status == domain.WagerProcessed || b.Status == domain.WagerRejected || b.Status == domain.WagerFailed {
		return domain.ErrWagerTerminal
	}
	if a.ID != b.ID || a.Origin != b.Origin || a.Request != b.Request || a.PayloadHash != b.PayloadHash ||
		!a.CreatedAt.Truncate(time.Microsecond).Equal(b.CreatedAt.Truncate(time.Microsecond)) || a.UpdatedAt.Before(b.UpdatedAt) ||
		(b.ReferenceTransactionID != "" && a.ReferenceTransactionID != b.ReferenceTransactionID) {
		return application.ErrInvalidWagerUpdate
	}
	current, err := r.read(ctx, "id=$1", true, b.ID)
	if err != nil {
		return err
	}
	c, err := current.Snapshot()
	if err != nil {
		return err
	}
	if persistedWagerState(c) != persistedWagerState(b) {
		return application.ErrWagerConflict
	}
	_, err = r.tx.Exec(ctx, `UPDATE public.wager_transactions SET reference_transaction_id=$1,status=$2,
 failure_code=$3,result_balance_minor=$4,updated_at=$5 WHERE id=$6`, nullableText(a.ReferenceTransactionID),
		a.Status, nullableText(string(a.FailureCode)), wagerResult(a), a.UpdatedAt, a.ID)
	if err != nil {
		return fmt.Errorf("update wager: %w", err)
	}
	return nil
}

// HasSuccessfulReversal enforces the policy shared by REFUND and ROLLBACK.
// The wallet lock serializes decisions; the partial unique index remains the final guard.
func (r *wagerRepository) HasSuccessfulReversal(ctx context.Context, referenceID string) (bool, error) {
	var found bool
	err := r.tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM public.wager_transactions
 WHERE reference_transaction_id=$1 AND status='PROCESSED' AND kind IN ('REFUND','ROLLBACK'))`, referenceID).Scan(&found)
	if err != nil {
		return false, fmt.Errorf("check successful reversal: %w", err)
	}
	return found, nil
}

// DueReferences reads candidates without locks; workers lock wallets before rechecking.
// NULL next_attempt_at from older records means immediately due at created_at.
func (r *wagerRepository) DueReferences(ctx context.Context, at time.Time, limit int) ([]application.ReferenceWork, error) {
	if limit < 1 || limit > 1000 {
		return nil, errors.New("invalid reference batch size")
	}
	rows, err := r.tx.Query(ctx, `SELECT id,wallet_id FROM public.wager_transactions
 WHERE status='PENDING_REFERENCE' AND COALESCE(next_attempt_at,created_at)<=$1
 ORDER BY COALESCE(next_attempt_at,created_at),id LIMIT $2`, at, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	work := []application.ReferenceWork{}
	for rows.Next() {
		var w application.ReferenceWork
		if err := rows.Scan(&w.TransactionID, &w.WalletID); err != nil {
			return nil, err
		}
		work = append(work, w)
	}
	return work, rows.Err()
}

// ReferenceWork reloads scheduling only for a still-pending operation under its wallet lock.
func (r *wagerRepository) ReferenceWork(ctx context.Context, id string) (application.ReferenceWork, error) {
	var w application.ReferenceWork
	err := r.tx.QueryRow(ctx, `SELECT t.id,t.wallet_id,t.attempts,COALESCE(t.next_attempt_at,t.created_at),
 e.correlation_id,e.event_id FROM public.wager_transactions t
 JOIN LATERAL (SELECT correlation_id,event_id FROM public.outbox_events
 WHERE aggregate_id=t.id AND event_type='WagerTransactionPendingReference'
 ORDER BY created_at,event_id LIMIT 1) e ON true
 WHERE t.id=$1 AND t.status='PENDING_REFERENCE'`, id).Scan(&w.TransactionID, &w.WalletID, &w.Attempts, &w.NextAttemptAt, &w.CorrelationID, &w.CausationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return w, application.ErrWagerNotFound
	}
	return w, err
}

// ScheduleReference advances durable attempts without changing financial state.
func (r *wagerRepository) ScheduleReference(ctx context.Context, id string, attempts int, next time.Time) error {
	tag, err := r.tx.Exec(ctx, `UPDATE public.wager_transactions SET attempts=$2,next_attempt_at=$3
 WHERE id=$1 AND status='PENDING_REFERENCE' AND attempts=$2-1`, id, attempts, next)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return application.ErrWagerConflict
	}
	return nil
}

// GetProviderWager applies ownership in SQL, including lookups by internal identity.
func (s *Store) GetProviderWager(ctx context.Context, provider, id string, external bool) (domain.WagerTransaction, error) {
	predicate := "provider_id=$1 AND id=$2"
	if external {
		predicate = "provider_id=$1 AND external_transaction_id=$2"
	}
	return readWager(ctx, s.pool, predicate, false, provider, id)
}
