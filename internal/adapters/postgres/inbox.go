package postgres

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/jackc/pgx/v5"
)

type inboxRepository struct{ tx pgx.Tx }

var _ application.InboxRepository = (*inboxRepository)(nil)

// Receive inserts without overwriting duplicates. Call GetForUpdate and compare hash
// before acknowledging any collision, including an already completed message.
func (r *inboxRepository) Receive(ctx context.Context, m application.InboxMessage) error {
	digest, err := hex.DecodeString(m.PayloadHash)
	if m.ConsumerName == "" || strings.TrimSpace(m.ConsumerName) != m.ConsumerName || m.MessageID == "" || strings.TrimSpace(m.MessageID) != m.MessageID ||
		err != nil || len(digest) != 32 || strings.ToLower(m.PayloadHash) != m.PayloadHash || m.ReceivedAt.IsZero() || m.CompletedAt != nil {
		return application.ErrInvalidInboxMessage
	}
	_, err = r.tx.Exec(ctx, `INSERT INTO public.inbox_messages(consumer_name,message_id,payload_hash,received_at)
 VALUES ($1,$2,$3,$4) ON CONFLICT (consumer_name,message_id) DO NOTHING`, m.ConsumerName, m.MessageID, m.PayloadHash, m.ReceivedAt)
	if err != nil {
		return fmt.Errorf("receive inbox message: %w", err)
	}
	return nil
}

// GetForUpdate serializes duplicate deliveries; the caller must acquire wallet locks afterwards.
func (r *inboxRepository) GetForUpdate(ctx context.Context, consumer, id string) (application.InboxMessage, error) {
	var m application.InboxMessage
	err := r.tx.QueryRow(ctx, `SELECT consumer_name,message_id,payload_hash,received_at,completed_at
 FROM public.inbox_messages WHERE consumer_name=$1 AND message_id=$2 FOR UPDATE`, consumer, id).Scan(&m.ConsumerName, &m.MessageID, &m.PayloadHash, &m.ReceivedAt, &m.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return m, application.ErrInboxNotFound
	}
	if err != nil {
		return m, fmt.Errorf("read inbox message: %w", err)
	}
	return m, nil
}

// Complete marks durable handling in the caller's transaction, including pending references.
func (r *inboxRepository) Complete(ctx context.Context, consumer, id string, at time.Time) error {
	if at.IsZero() {
		return application.ErrInvalidInboxMessage
	}
	tag, err := r.tx.Exec(ctx, `UPDATE public.inbox_messages SET completed_at=$3
 WHERE consumer_name=$1 AND message_id=$2 AND completed_at IS NULL`, consumer, id, at)
	if err != nil {
		return fmt.Errorf("complete inbox message: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return application.ErrInvalidInboxMessage
	}
	return nil
}
