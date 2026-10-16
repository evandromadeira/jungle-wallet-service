package application

import (
	"context"
	"errors"
	"github.com/evandromadeira/jungle-wallet-service/internal/telemetry"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

// ReferencePolicy bounds the lifetime and exponential retry delay of pending references.
type ReferencePolicy struct {
	BatchSize    int
	MaxAttempts  int
	InitialDelay time.Duration
	MaxDelay     time.Duration
	TTL          time.Duration
	PollInterval time.Duration
}

// ReferenceWorker has no in-memory ownership; each transaction rechecks durable state.
type ReferenceWorker struct {
	processor *ProcessWager
	policy    ReferencePolicy
}

// NewReferenceWorker validates bounded scheduling without starting a background goroutine.
func NewReferenceWorker(processor *ProcessWager, p ReferencePolicy) (*ReferenceWorker, error) {
	if processor == nil || p.BatchSize < 1 || p.BatchSize > 1000 || p.MaxAttempts < 1 || p.MaxAttempts > 1000000 ||
		p.InitialDelay < time.Microsecond || p.MaxDelay < p.InitialDelay || p.TTL <= 0 || p.PollInterval <= 0 {
		return nil, errors.New("invalid reference worker configuration")
	}
	return &ReferenceWorker{processor, p}, nil
}

// delay grows exponentially with saturation, avoiding duration overflow.
func (w *ReferenceWorker) delay(attempt int) time.Duration {
	d := w.policy.InitialDelay
	for i := 1; i < attempt && d < w.policy.MaxDelay; i++ {
		if d > w.policy.MaxDelay/2 {
			return w.policy.MaxDelay
		}
		d *= 2
	}
	if d > w.policy.MaxDelay {
		return w.policy.MaxDelay
	}
	return d
}

// Run polls until cancellation; its owner must observe errors and manage lifecycle.
// A failed batch is reported, not silently swallowed. No goroutine outlives Run.
func (w *ReferenceWorker) Run(ctx context.Context) error {
	for {
		if _, err := w.RunOnce(ctx); err != nil {
			return err
		}
		timer := time.NewTimer(w.policy.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// RunOnce handles due records with per-wallet locking and atomic resolution/expiry.
// Multiple workers may select the same candidate; only one can change it while due.
func (w *ReferenceWorker) RunOnce(ctx context.Context) (int, error) {
	u := w.processor
	at := u.now().Round(0).UTC().Truncate(time.Microsecond)
	var candidates []ReferenceWork
	err := u.work.WithinTransaction(ctx, func(r Repositories) error {
		var e error
		candidates, e = r.Wagers.DueReferences(ctx, at, w.policy.BatchSize)
		return e
	})
	if err != nil {
		return 0, err
	}
	count := 0
	for _, candidate := range candidates {
		changed := false
		start := time.Now()
		var observed WagerResult
		var correlation, message, provider string
		err = u.work.WithinTransaction(ctx, func(r Repositories) error {
			before, e := r.Wallets.GetForUpdate(ctx, candidate.WalletID)
			if e != nil {
				return e
			}
			current, e := r.Wagers.Get(ctx, candidate.TransactionID)
			if e != nil {
				return e
			}
			s, e := current.Snapshot()
			if e != nil {
				return e
			}
			if s.Status != domain.WagerPendingReference {
				return nil
			}
			work, e := r.Wagers.ReferenceWork(ctx, candidate.TransactionID)
			if e != nil {
				return e
			}
			now := u.now().Round(0).UTC().Truncate(time.Microsecond)
			if work.NextAttemptAt.After(now) {
				return nil
			}
			bs, e := before.Snapshot()
			if e != nil {
				return e
			}
			at := now
			if at.Before(s.UpdatedAt) {
				at = s.UpdatedAt
			}
			if at.Before(bs.UpdatedAt) {
				at = bs.UpdatedAt
			}
			// Recheck the reference under the wallet lock before enforcing expiry.
			// A reference may have completed while this worker was unavailable.
			after, finished, e := evaluateWager(ctx, r, current, before, at)
			if e != nil {
				return e
			}
			outcome, e := finished.Snapshot()
			if e != nil {
				return e
			}
			attempt := work.Attempts + 1
			if outcome.Status == domain.WagerPendingReference && (attempt >= w.policy.MaxAttempts || !now.Before(s.CreatedAt.Add(w.policy.TTL))) {
				finished, e = current.Reject(domain.FailureReferenceNotFound, &bs.Balance, at)
				if e != nil {
					return e
				}
				outcome, _ = finished.Snapshot()
			}
			next := now.Add(w.delay(attempt))
			expiry := s.CreatedAt.Add(w.policy.TTL)
			if expiry.Before(next) {
				next = expiry
			}
			// Advance retry metadata before any terminal transition, in the same transaction.
			if e = r.Wagers.ScheduleReference(ctx, s.ID, attempt, next); e != nil {
				return e
			}
			if e = r.Wagers.Update(ctx, current, finished); e != nil {
				return e
			}
			if outcome.Status != domain.WagerPendingReference {
				if e = u.writeOutcome(ctx, r, before, after, finished, work.CorrelationID, work.CausationID); e != nil {
					return e
				}
			}
			observed = WagerResult{Transaction: finished}
			correlation = work.CorrelationID
			message = work.CausationID
			provider = s.Request.ProviderID
			changed = true
			return nil
		})
		if err != nil {
			return count, err
		}
		if changed {
			telemetry.From(ctx).Count("wallet_reference_retries_total")
			observeWager(ctx, "reference", start, observed, nil, correlation, message, provider, candidate.WalletID)
			count++
		}
	}
	return count, nil
}
