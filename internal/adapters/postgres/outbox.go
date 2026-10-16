package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type outboxRepository struct{ tx pgx.Tx }

var _ application.OutboxRepository = (*outboxRepository)(nil)

// Append persists constructor-created event values with untouched delivery defaults.
// Identity collisions are errors: callers must roll back, not discard required events.
func (r *outboxRepository) Append(ctx context.Context, event application.OutboxEvent) error {
	switch event.(type) {
	case domain.WagerTransactionProcessed, domain.WagerTransactionRejected,
		domain.WagerTransactionPendingReference, domain.WalletBalanceChanged:
	default:
		return domain.ErrInvalidEvent
	}
	s, err := event.Snapshot()
	if err != nil {
		return err
	}
	_, err = r.tx.Exec(ctx, `INSERT INTO public.outbox_events
 (event_id,aggregate_id,event_type,correlation_id,causation_id,version,payload,occurred_at)
 VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8)`, s.EventID, s.AggregateID, s.EventType, s.CorrelationID,
		nullableText(s.CausationID), s.Version, s.Payload, s.OccurredAt)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return application.ErrOutboxAlreadyExists
	}
	if err != nil {
		return fmt.Errorf("append outbox event: %w", err)
	}
	return nil
}

// Get reads the stored snapshot, never reconstructing it from current wallet state.
// JSONB may change whitespace/key order; monetary strings and JSON values are preserved.
func (r *outboxRepository) Get(ctx context.Context, id string) (domain.EventSnapshot, error) {
	var s domain.EventSnapshot
	err := r.tx.QueryRow(ctx, `SELECT event_id,event_type,aggregate_id,correlation_id,COALESCE(causation_id,''),
 occurred_at,version,payload::text FROM public.outbox_events WHERE event_id=$1`, id).Scan(
		&s.EventID, &s.EventType, &s.AggregateID, &s.CorrelationID, &s.CausationID, &s.OccurredAt, &s.Version, &s.Payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.EventSnapshot{}, application.ErrOutboxNotFound
	}
	if err != nil {
		return domain.EventSnapshot{}, fmt.Errorf("read outbox event: %w", err)
	}
	s.OccurredAt = s.OccurredAt.UTC()
	return s, nil
}

// ClaimDue reserves one due event until commit/rollback, skipping locks held by peers.
func (r *outboxRepository) ClaimDue(ctx context.Context, at time.Time) (application.OutboxDelivery, error) {
	var item application.OutboxDelivery
	var id string
	err := r.tx.QueryRow(ctx, `SELECT event_id,attempts FROM public.outbox_events
 WHERE published_at IS NULL AND next_attempt_at<=$1
 ORDER BY next_attempt_at,event_id LIMIT 1 FOR UPDATE SKIP LOCKED`, at).Scan(&id, &item.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return item, application.ErrOutboxNotFound
	}
	if err != nil {
		return item, fmt.Errorf("claim outbox event: %w", err)
	}
	item.Event, err = r.Get(ctx, id)
	return item, err
}

// FinishDelivery changes delivery metadata only; callers must hold the row lock.
// Attempts saturate at the SQL integer limit so retryable events never become stuck.
func (r *outboxRepository) FinishDelivery(ctx context.Context, id string, published bool, at time.Time) error {
	tag, err := r.tx.Exec(ctx, `UPDATE public.outbox_events SET
 attempts=CASE WHEN attempts<2147483647 THEN attempts+1 ELSE attempts END,
 published_at=CASE WHEN $2 THEN GREATEST($3::timestamptz,created_at) ELSE NULL END,
 next_attempt_at=CASE WHEN $2 THEN next_attempt_at ELSE $3::timestamptz END,
 last_error=CASE WHEN $2 THEN NULL ELSE 'delivery_failed' END
 WHERE event_id=$1 AND published_at IS NULL`, id, published, at)
	if err != nil {
		return fmt.Errorf("finish outbox delivery: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return application.ErrOutboxNotFound
	}
	return nil
}
