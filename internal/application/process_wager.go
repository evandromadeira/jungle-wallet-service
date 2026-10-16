package application

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

var (
	ErrInvalidWagerCommand = errors.New("invalid wager command or dependencies")
	ErrProviderMismatch    = errors.New("request provider differs from authorized provider")
	ErrWagerWalletMismatch = errors.New("wager player or currency differs from wallet")
	ErrWagerPending        = errors.New("existing transaction is not terminal")
)

// WagerCommand carries a provider already authenticated by the trusted entry point.
// AuthorizedProviderID is not a body field and must never be trusted from client input.
type WagerCommand struct {
	AuthorizedProviderID string
	Request              domain.WagerRequest
	CorrelationID        string
	CausationID          string
}

// WagerResult returns the persisted original outcome, including business rejections.
type WagerResult struct {
	Transaction      domain.WagerTransaction
	IdempotentReplay bool
}

// ProcessWager coordinates all five external operations, persisting unresolved references for recovery.
type ProcessWager struct {
	work  UnitOfWork
	newID func() (string, error)
	now   func() time.Time
}

// NewProcessWager injects transaction scope, concurrency-safe identity generation and clock.
func NewProcessWager(work UnitOfWork, newID func() (string, error), now func() time.Time) (*ProcessWager, error) {
	if work == nil || newID == nil || now == nil {
		return nil, ErrInvalidWagerCommand
	}
	return &ProcessWager{work, newID, now}, nil
}

// findWagerReplay checks both unique identities, rejecting collisions across operations.
// An alternative key may replay the same external operation but is not stored as an alias.
func findWagerReplay(ctx context.Context, r WagerRepository, q domain.WagerRequest) (domain.WagerTransaction, bool, error) {
	byKey, ke := r.GetByIdempotencyKey(ctx, q.ProviderID, q.IdempotencyKey)
	if ke != nil && !errors.Is(ke, ErrWagerNotFound) {
		return domain.WagerTransaction{}, false, ke
	}
	byExternal, ee := r.GetByExternalID(ctx, q.ProviderID, q.ExternalTransactionID)
	if ee != nil && !errors.Is(ee, ErrWagerNotFound) {
		return domain.WagerTransaction{}, false, ee
	}
	if ke != nil && ee != nil {
		return domain.WagerTransaction{}, false, nil
	}
	existing := byExternal
	if ke == nil {
		existing = byKey
	}
	if ke == nil && ee == nil {
		k, _ := byKey.Snapshot()
		e, _ := byExternal.Snapshot()
		if k.ID != e.ID {
			return domain.WagerTransaction{}, false, domain.ErrWagerPayloadConflict
		}
	}
	if err := existing.CheckPayload(q); err != nil {
		return domain.WagerTransaction{}, false, err
	}
	s, err := existing.Snapshot()
	if err != nil {
		return domain.WagerTransaction{}, false, err
	}
	if s.Status == domain.WagerPending {
		return domain.WagerTransaction{}, false, ErrWagerPending
	}
	return existing, true, nil
}

// Execute locks one wallet, rechecks identities after acquiring the lock, and commits
// outcome, balance, ledger and events together. Errors expose no uncommitted result.
// No external publication or automatic retry (including ambiguous commits) occurs here.
func (u *ProcessWager) Execute(ctx context.Context, c WagerCommand) (observed WagerResult, observedErr error) {
	start := time.Now()
	defer func() {
		observeWager(ctx, "http_wager", start, observed, observedErr, c.CorrelationID, c.CausationID, c.AuthorizedProviderID, c.Request.WalletID)
	}()
	pending, err := u.prepare(ctx, c)
	if err != nil {
		return WagerResult{}, err
	}
	var result WagerResult
	err = u.work.WithinTransaction(ctx, func(r Repositories) error { var e error; result, e = u.processWithin(ctx, r, c, pending); return e })
	if err != nil {
		return WagerResult{}, err
	}
	return result, nil
}

// prepare validates trusted metadata and business input before opening a transaction.
func (u *ProcessWager) prepare(ctx context.Context, c WagerCommand) (domain.WagerTransaction, error) {
	if err := ctx.Err(); err != nil {
		return domain.WagerTransaction{}, err
	}
	if c.AuthorizedProviderID == "" || c.AuthorizedProviderID != c.Request.ProviderID {
		return domain.WagerTransaction{}, ErrProviderMismatch
	}
	if (c.Request.Kind != domain.WagerBet && c.Request.Kind != domain.WagerWin && c.Request.Kind != domain.WagerLoss && c.Request.Kind != domain.WagerRefund && c.Request.Kind != domain.WagerRollback) || c.CorrelationID == "" || strings.TrimSpace(c.CorrelationID) != c.CorrelationID ||
		(c.CausationID != "" && strings.TrimSpace(c.CausationID) != c.CausationID) {
		return domain.WagerTransaction{}, ErrInvalidWagerCommand
	}
	id, err := u.newID()
	if err != nil {
		return domain.WagerTransaction{}, err
	}
	created := u.now().Round(0).UTC().Truncate(time.Microsecond)
	pending, err := domain.NewWagerTransaction(id, c.Request, created)
	if err != nil {
		return domain.WagerTransaction{}, err
	}
	return pending, nil
}

// processWithin shares the caller's transaction, including inbox processing.
func (u *ProcessWager) processWithin(ctx context.Context, r Repositories, c WagerCommand, pending domain.WagerTransaction) (WagerResult, error) {
	state, err := pending.Snapshot()
	if err != nil {
		return WagerResult{}, err
	}
	created := state.CreatedAt
	var result WagerResult
	err = func() error {
		existing, found, e := findWagerReplay(ctx, r.Wagers, c.Request)
		if e != nil {
			return e
		}
		if found {
			result = WagerResult{existing, true}
			return nil
		}
		before, e := r.Wallets.GetForUpdate(ctx, c.Request.WalletID)
		if e != nil {
			return e
		}
		existing, found, e = findWagerReplay(ctx, r.Wagers, c.Request)
		if e != nil {
			return e
		}
		if found {
			result = WagerResult{existing, true}
			return nil
		}
		bs, e := before.Snapshot()
		if e != nil {
			return e
		}
		wc, _ := bs.Balance.Currency()
		qc, _ := c.Request.Money.Currency()
		if bs.PlayerID != c.Request.PlayerID || wc != qc {
			return ErrWagerWalletMismatch
		}
		at := u.now().Round(0).UTC().Truncate(time.Microsecond)
		if at.Before(created) {
			at = created
		}
		if at.Before(bs.UpdatedAt) {
			at = bs.UpdatedAt
		}
		after, finished, e := evaluateWager(ctx, r, pending, before, at)
		if e != nil {
			return e
		}
		inserted, e := r.Wagers.Create(ctx, finished)
		if e != nil {
			return e
		}
		if !inserted {
			existing, found, e = findWagerReplay(ctx, r.Wagers, c.Request)
			if e != nil {
				return e
			}
			if !found {
				return ErrWagerConflict
			}
			result = WagerResult{existing, true}
			return nil
		}
		if e = u.writeOutcome(ctx, r, before, after, finished, c.CorrelationID, c.CausationID); e != nil {
			return e
		}
		result = WagerResult{finished, false}
		return nil
	}()
	if err != nil {
		return WagerResult{}, err
	}
	return result, nil
}

// evaluateWager computes outcomes without writes for both incoming operations and reference recovery.
func evaluateWager(ctx context.Context, r Repositories, pending domain.WagerTransaction, before domain.Wallet, at time.Time) (domain.Wallet, domain.WagerTransaction, error) {
	s, err := pending.Snapshot()
	if err != nil {
		return domain.Wallet{}, domain.WagerTransaction{}, err
	}
	bs, err := before.Snapshot()
	if err != nil {
		return domain.Wallet{}, domain.WagerTransaction{}, err
	}
	if s.Request.ReferenceExternalTransactionID != "" {
		reference, e := r.Wagers.GetByExternalID(ctx, s.Request.ProviderID, s.Request.ReferenceExternalTransactionID)
		if errors.Is(e, ErrWagerNotFound) {
			wait, e := pending.WaitForReference(at)
			return before, wait, e
		}
		if e != nil {
			return domain.Wallet{}, domain.WagerTransaction{}, e
		}
		resolved, e := pending.ResolveReference(reference, at)
		switch {
		case errors.Is(e, domain.ErrWagerReferencePending):
			wait, e := pending.WaitForReference(at)
			return before, wait, e
		case errors.Is(e, domain.ErrWagerReferenceUnsuccessful):
			rejected, e := pending.Reject(domain.FailureReferenceUnsuccessful, &bs.Balance, at)
			return before, rejected, e
		case errors.Is(e, domain.ErrWagerReferenceMismatch):
			rejected, e := pending.Reject(domain.FailureReferenceMismatch, &bs.Balance, at)
			return before, rejected, e
		case e != nil:
			return domain.Wallet{}, domain.WagerTransaction{}, e
		}
		pending = resolved
		if s.Request.Kind == domain.WagerRefund || s.Request.Kind == domain.WagerRollback {
			original, _ := reference.Snapshot()
			reversed, e := r.Wagers.HasSuccessfulReversal(ctx, original.ID)
			if e != nil {
				return domain.Wallet{}, domain.WagerTransaction{}, e
			}
			if reversed {
				rejected, e := pending.Reject(domain.FailureAlreadyReversed, &bs.Balance, at)
				return before, rejected, e
			}
		}
	}
	after := before
	switch s.Request.Kind {
	case domain.WagerBet:
		after, err = before.Debit(s.Request.Money, at)
	case domain.WagerWin, domain.WagerRefund:
		after, err = before.Credit(s.Request.Money, at)
	case domain.WagerRollback:
		resolved, _ := pending.Snapshot()
		original, e := r.Ledger.GetByTransaction(ctx, s.Request.WalletID, resolved.ReferenceTransactionID)
		// A committed, reversible financial transaction must have its immutable ledger entry.
		// A successful lookup reporting absence is a permanent integrity failure, not a timeout.
		if errors.Is(e, ErrLedgerNotFound) {
			failed, err := pending.Fail(&bs.Balance, at)
			return before, failed, err
		}
		if e != nil {
			return domain.Wallet{}, domain.WagerTransaction{}, e
		}
		entry, e := original.Snapshot()
		if e != nil {
			return domain.Wallet{}, domain.WagerTransaction{}, e
		}
		if entry.Money != s.Request.Money {
			return domain.Wallet{}, domain.WagerTransaction{}, domain.ErrLedgerTransactionMismatch
		}
		if entry.Direction == domain.LedgerDebit {
			after, err = before.Credit(s.Request.Money, at)
		} else {
			after, err = before.Debit(s.Request.Money, at)
		}
	}
	if errors.Is(err, domain.ErrInsufficientFunds) {
		code := domain.FailureInsufficientFunds
		if s.Request.Kind == domain.WagerRollback {
			code = domain.FailureReversalInsufficientFunds
		}
		rejected, e := pending.Reject(code, &bs.Balance, at)
		return before, rejected, e
	}
	if errors.Is(err, domain.ErrMoneyOverflow) {
		rejected, e := pending.Reject(domain.FailureBalanceOverflow, &bs.Balance, at)
		return before, rejected, e
	}
	if err != nil {
		return domain.Wallet{}, domain.WagerTransaction{}, err
	}
	as, _ := after.Snapshot()
	finished, err := pending.MarkProcessed(as.Balance, at)
	return after, finished, err
}

// writeOutcome persists balance, ledger and events in the caller's transaction.
func (u *ProcessWager) writeOutcome(ctx context.Context, r Repositories, before, after domain.Wallet, finished domain.WagerTransaction, correlationID, causationID string) error {
	outcome, e := finished.Snapshot()
	if e != nil {
		return e
	}
	bs, e := before.Snapshot()
	if e != nil {
		return e
	}
	// FAILED is an audited terminal transaction without a financial movement.
	// It must not be published as a business rejection or successful movement.
	if outcome.Status == domain.WagerFailed {
		return nil
	}
	at := outcome.UpdatedAt
	eventID, e := u.newID()
	if e != nil {
		return e
	}
	metadata := domain.EventMetadata{EventID: eventID, CorrelationID: correlationID, CausationID: causationID}
	if outcome.Status == domain.WagerPendingReference {
		event, e := domain.NewWagerTransactionPendingReference(metadata, finished)
		if e != nil {
			return e
		}
		if e = r.Outbox.Append(ctx, event); e != nil {
			return e
		}
	} else if outcome.Status == domain.WagerRejected {
		event, e := domain.NewWagerTransactionRejected(metadata, finished)
		if e != nil {
			return e
		}
		if e = r.Outbox.Append(ctx, event); e != nil {
			return e
		}
	} else if outcome.Request.Kind == domain.WagerLoss {
		event, e := domain.NewWagerTransactionProcessed(metadata, finished)
		if e != nil {
			return e
		}
		if e = r.Outbox.Append(ctx, event); e != nil {
			return e
		}
	} else {
		if e = r.Wallets.Update(ctx, before, after); e != nil {
			return e
		}
		ledgerID, e := u.newID()
		if e != nil {
			return e
		}
		as, _ := after.Snapshot()
		direction := domain.LedgerDebit
		comparison, _ := as.Balance.Compare(bs.Balance)
		if comparison > 0 {
			direction = domain.LedgerCredit
		}
		entry, e := domain.NewWalletLedgerEntry(ledgerID, finished, direction, bs.Balance, as.Balance, at)
		if e != nil {
			return e
		}
		if e = r.Ledger.Append(ctx, entry); e != nil {
			return e
		}
		event, e := domain.NewWagerTransactionProcessed(metadata, finished)
		if e != nil {
			return e
		}
		if e = r.Outbox.Append(ctx, event); e != nil {
			return e
		}
		metadata.EventID, e = u.newID()
		if e != nil {
			return e
		}
		changed, e := domain.NewWalletBalanceChanged(metadata, finished, entry, after)
		if e != nil {
			return e
		}
		if e = r.Outbox.Append(ctx, changed); e != nil {
			return e
		}
	}

	return nil
}
