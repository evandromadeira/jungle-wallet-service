package domain

import (
	"errors"
	"math"
	"strings"
	"time"
)

var (
	ErrInvalidWallet         = errors.New("invalid or uninitialized wallet")
	ErrInvalidWalletID       = errors.New("invalid wallet identity")
	ErrInvalidPlayerID       = errors.New("invalid player identity")
	ErrInvalidWalletVersion  = errors.New("invalid wallet version")
	ErrInvalidWalletTime     = errors.New("invalid wallet timestamp")
	ErrNegativeBalance       = errors.New("negative wallet balance")
	ErrInvalidMovement       = errors.New("negative wallet movement")
	ErrInsufficientFunds     = errors.New("insufficient wallet funds")
	ErrWalletVersionOverflow = errors.New("wallet version overflow")
)

// Wallet holds an immutable in-memory financial state; its zero value is invalid.
// Database transactions must enforce concurrency, uniqueness, and ledger atomicity.
type Wallet struct {
	state WalletState
}

// WalletState is a detached snapshot for persistence, not a mutable wallet.
// Currency is carried by Balance to avoid conflicting currency fields.
type WalletState struct {
	ID        string
	PlayerID  string
	Balance   Money
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

// NewWallet creates a wallet at version 1, without replaying a credit operation.
// Positive opening balances still require an opening transaction and ledger entry
// in the application/database layer. IDs and the clock are supplied by the caller.
func NewWallet(id, playerID string, initialBalance Money, at time.Time) (Wallet, error) {
	return RehydrateWallet(WalletState{
		ID: id, PlayerID: playerID, Balance: initialBalance,
		Version: 1, CreatedAt: at, UpdatedAt: at,
	})
}

// RehydrateWallet restores and validates stored state without moving money,
// incrementing the version, or emitting events. Timestamps are normalized to UTC.
func RehydrateWallet(state WalletState) (Wallet, error) {
	state.CreatedAt = state.CreatedAt.Round(0).UTC()
	state.UpdatedAt = state.UpdatedAt.Round(0).UTC()
	wallet := Wallet{state: state}
	if err := wallet.Validate(); err != nil {
		return Wallet{}, err
	}
	return wallet, nil
}

// Validate checks identity, balance, version, and timestamp invariants.
// IDs are opaque nonblank strings without surrounding whitespace; transport
// adapters may enforce a stricter ID format.
func (w Wallet) Validate() error {
	if w == (Wallet{}) {
		return ErrInvalidWallet
	}
	if w.state.ID == "" || strings.TrimSpace(w.state.ID) != w.state.ID {
		return ErrInvalidWalletID
	}
	if w.state.PlayerID == "" || strings.TrimSpace(w.state.PlayerID) != w.state.PlayerID {
		return ErrInvalidPlayerID
	}
	minor, err := w.state.Balance.Minor()
	if err != nil {
		return err
	}
	if minor < 0 {
		return ErrNegativeBalance
	}
	if w.state.Version < 1 {
		return ErrInvalidWalletVersion
	}
	if w.state.CreatedAt.IsZero() || w.state.UpdatedAt.IsZero() || w.state.UpdatedAt.Before(w.state.CreatedAt) {
		return ErrInvalidWalletTime
	}
	return nil
}

// Snapshot returns a validated copy of the state; editing it cannot change w.
// Callers must check the error before using the returned state.
func (w Wallet) Snapshot() (WalletState, error) {
	if err := w.Validate(); err != nil {
		return WalletState{}, err
	}
	return w.state, nil
}

// Debit returns a new wallet with the amount deducted, or rejects insufficient funds.
// Zero is a no-op. Negative amounts and currency mismatches are rejected.
func (w Wallet) Debit(amount Money, at time.Time) (Wallet, error) {
	return w.move(amount, at, true)
}

// Credit returns a new wallet with the amount added, rejecting monetary overflow.
// Zero is a no-op. Negative amounts and currency mismatches are rejected.
func (w Wallet) Credit(amount Money, at time.Time) (Wallet, error) {
	return w.move(amount, at, false)
}

// move validates all inputs before producing a new state. Every balance change
// increments the version exactly once; failed operations leave the original intact.
// One snapshot must belong to one unit of work; no local lock replaces database locking.
func (w Wallet) move(amount Money, at time.Time, debit bool) (Wallet, error) {
	if err := w.Validate(); err != nil {
		return Wallet{}, err
	}
	if err := w.state.Balance.sameCurrency(amount); err != nil {
		return Wallet{}, err
	}
	if amount.minor < 0 {
		return Wallet{}, ErrInvalidMovement
	}
	at = at.Round(0).UTC()
	if at.IsZero() || at.Before(w.state.UpdatedAt) {
		return Wallet{}, ErrInvalidWalletTime
	}
	if amount.minor == 0 {
		return w, nil
	}
	if debit && amount.minor > w.state.Balance.minor {
		return Wallet{}, ErrInsufficientFunds
	}
	var balance Money
	var err error
	if debit {
		balance, err = w.state.Balance.Sub(amount)
	} else {
		balance, err = w.state.Balance.Add(amount)
	}
	if err != nil {
		return Wallet{}, err
	}
	if w.state.Version == math.MaxInt64 {
		return Wallet{}, ErrWalletVersionOverflow
	}
	next := w.state
	next.Balance = balance
	next.Version++
	next.UpdatedAt = at
	return RehydrateWallet(next)
}
