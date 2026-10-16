package domain

import (
	"errors"
	"strings"
	"time"
)

type LedgerDirection string

const (
	LedgerDebit  LedgerDirection = "DEBIT"
	LedgerCredit LedgerDirection = "CREDIT"
)

var (
	ErrInvalidLedgerEntry        = errors.New("invalid ledger entry")
	ErrInvalidLedgerDirection    = errors.New("invalid ledger direction")
	ErrLedgerBalanceMismatch     = errors.New("ledger balance mismatch")
	ErrLedgerTransactionMismatch = errors.New("ledger transaction mismatch")
)

// WalletLedgerEntry is an immutable financial record. Database constraints must
// also prevent duplicate entries and block UPDATE and DELETE.
type WalletLedgerEntry struct{ state WalletLedgerState }

// WalletLedgerState is a detached persistence snapshot with no shared mutable fields.
type WalletLedgerState struct {
	ID            string
	WalletID      string
	TransactionID string
	Direction     LedgerDirection
	Money         Money
	BalanceBefore Money
	BalanceAfter  Money
	CreatedAt     time.Time
}

// NewWalletLedgerEntry creates a record for a processed, balance-changing transaction.
// ROLLBACK direction must be selected by the use case from the resolved original
// movement. This constructor verifies the arithmetic, not the original ledger row.
func NewWalletLedgerEntry(id string, tx WagerTransaction, direction LedgerDirection, before, after Money, at time.Time) (WalletLedgerEntry, error) {
	s, err := tx.Snapshot()
	if err != nil {
		return WalletLedgerEntry{}, err
	}
	if s.Status != WagerProcessed || s.Request.Kind == WagerLoss {
		return WalletLedgerEntry{}, ErrLedgerTransactionMismatch
	}
	if s.Request.Kind == WagerBet && direction != LedgerDebit {
		return WalletLedgerEntry{}, ErrInvalidLedgerDirection
	}
	if (s.Request.Kind == WagerWin || s.Request.Kind == WagerRefund || s.Request.Kind == WagerOpening) && direction != LedgerCredit {
		return WalletLedgerEntry{}, ErrInvalidLedgerDirection
	}
	if s.ResultBalance != after || at.Before(s.UpdatedAt) {
		return WalletLedgerEntry{}, ErrLedgerTransactionMismatch
	}
	if s.Request.Kind == WagerOpening && before.minor != 0 {
		return WalletLedgerEntry{}, ErrLedgerBalanceMismatch
	}
	return RehydrateWalletLedgerEntry(WalletLedgerState{ID: id, WalletID: s.Request.WalletID, TransactionID: s.ID,
		Direction: direction, Money: s.Request.Money, BalanceBefore: before, BalanceAfter: after, CreatedAt: at})
}

// RehydrateWalletLedgerEntry validates persisted data without applying a movement.
// The repository must verify foreign keys and linkage to the transaction.
func RehydrateWalletLedgerEntry(s WalletLedgerState) (WalletLedgerEntry, error) {
	s.CreatedAt = s.CreatedAt.Round(0).UTC()
	entry := WalletLedgerEntry{state: s}
	if err := entry.Validate(); err != nil {
		return WalletLedgerEntry{}, err
	}
	return entry, nil
}

// Validate checks positive movement, nonnegative balances, matching currencies,
// and the exact balance equation without floating point or unchecked overflow.
func (e WalletLedgerEntry) Validate() error {
	s := e.state
	for _, id := range []string{s.ID, s.WalletID, s.TransactionID} {
		if id == "" || strings.TrimSpace(id) != id {
			return ErrInvalidLedgerEntry
		}
	}
	if s.CreatedAt.IsZero() {
		return ErrInvalidLedgerEntry
	}
	if err := s.Money.sameCurrency(s.BalanceBefore); err != nil {
		return err
	}
	if err := s.Money.sameCurrency(s.BalanceAfter); err != nil {
		return err
	}
	if s.Money.minor <= 0 {
		return ErrInvalidAmount
	}
	if s.BalanceBefore.minor < 0 || s.BalanceAfter.minor < 0 {
		return ErrNegativeBalance
	}
	var expected Money
	var err error
	switch s.Direction {
	case LedgerDebit:
		if s.Money.minor > s.BalanceBefore.minor {
			return ErrInsufficientFunds
		}
		expected, err = s.BalanceBefore.Sub(s.Money)
	case LedgerCredit:
		expected, err = s.BalanceBefore.Add(s.Money)
	default:
		return ErrInvalidLedgerDirection
	}
	if err != nil {
		return err
	}
	if expected != s.BalanceAfter {
		return ErrLedgerBalanceMismatch
	}
	return nil
}

// Snapshot returns a validated copy; editing it cannot modify the original entry.
func (e WalletLedgerEntry) Snapshot() (WalletLedgerState, error) {
	if err := e.Validate(); err != nil {
		return WalletLedgerState{}, err
	}
	return e.state, nil
}
