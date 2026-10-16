package application

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/evandromadeira/jungle-wallet-service/internal/telemetry"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

// EventSender must honor cancellation and return nil only after broker acceptance.
// Consumers must deduplicate by eventId: successful sends can be repeated.
type EventSender interface {
	Send(context.Context, []byte) error
}

// OutboxDelivery carries the immutable event and durable retry count.
type OutboxDelivery struct {
	Event    domain.EventSnapshot
	Attempts int
}

// OutboxPolicy bounds batches, network calls and exponential retry delays.
type OutboxPolicy struct {
	BatchSize                                         int
	SendTimeout, InitialDelay, MaxDelay, PollInterval time.Duration
}

// OutboxPublisher publishes only previously committed events under a database lock.
type OutboxPublisher struct {
	work   UnitOfWork
	sender EventSender
	now    func() time.Time
	policy OutboxPolicy
}

// NewOutboxPublisher validates dependencies without starting background work.
func NewOutboxPublisher(work UnitOfWork, sender EventSender, now func() time.Time, p OutboxPolicy) (*OutboxPublisher, error) {
	if work == nil || sender == nil || now == nil || p.BatchSize < 1 || p.BatchSize > 1000 || p.SendTimeout <= 0 || p.InitialDelay < time.Microsecond || p.MaxDelay < p.InitialDelay || p.PollInterval <= 0 {
		return nil, errors.New("invalid outbox publisher configuration")
	}
	return &OutboxPublisher{work, sender, now, p}, nil
}

// delay saturates before multiplication can overflow. Events are never discarded.
func (p *OutboxPublisher) delay(attempts int) time.Duration {
	d := p.policy.InitialDelay
	for i := 0; i < attempts && d < p.policy.MaxDelay; i++ {
		if d > p.policy.MaxDelay/2 {
			return p.policy.MaxDelay
		}
		d *= 2
	}
	if d > p.policy.MaxDelay {
		return p.policy.MaxDelay
	}
	return d
}

// RunOnce returns committed delivery attempts, including scheduled retries.
// A successful send followed by rollback is retried with the same stored envelope.
func (p *OutboxPublisher) RunOnce(ctx context.Context) (int, error) {
	count := 0
	for count < p.policy.BatchSize {
		found := false
		start := time.Now()
		var event domain.EventSnapshot
		status := "ERROR"
		err := p.work.WithinTransaction(ctx, func(r Repositories) error {
			item, e := r.Outbox.ClaimDue(ctx, p.now().UTC())
			if errors.Is(e, ErrOutboxNotFound) {
				return nil
			}
			if e != nil {
				return e
			}
			found = true
			event = item.Event
			body, e := json.Marshal(struct {
				domain.EventSnapshot
				Data json.RawMessage `json:"data"`
			}{item.Event, json.RawMessage(item.Event.Payload)})
			if e != nil {
				return e
			}
			sendCtx, cancel := context.WithTimeout(ctx, p.policy.SendTimeout)
			e = p.sender.Send(sendCtx, body)
			// A sender returning success after its deadline leaves acceptance uncertain.
			if e == nil {
				e = sendCtx.Err()
			}
			cancel()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			at := p.now().UTC().Truncate(time.Microsecond)
			if e != nil {
				status = "RETRY"
				return r.Outbox.FinishDelivery(ctx, item.Event.EventID, false, at.Add(p.delay(item.Attempts)))
			}
			status = "PUBLISHED"
			return r.Outbox.FinishDelivery(ctx, item.Event.EventID, true, at)
		})
		if err != nil {
			return count, err
		}
		if !found {
			break
		}
		if status == "RETRY" {
			telemetry.From(ctx).Count("wallet_outbox_retries_total")
		}
		telemetry.From(ctx).Observe(ctx, "outbox", status, start, "eventId", event.EventID, "correlationId", event.CorrelationID, "messageId", event.CausationID, "aggregateId", event.AggregateID)
		count++
	}
	return count, nil
}

// Run polls until cancellation or a database failure; its owner supervises failures.
func (p *OutboxPublisher) Run(ctx context.Context) error {
	for {
		if _, err := p.RunOnce(ctx); err != nil {
			return err
		}
		timer := time.NewTimer(p.policy.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
