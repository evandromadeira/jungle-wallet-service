package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type WagerKind string

const (
	WagerOpening  WagerKind = "OPENING"
	WagerBet      WagerKind = "BET"
	WagerWin      WagerKind = "WIN"
	WagerLoss     WagerKind = "LOSS"
	WagerRefund   WagerKind = "REFUND"
	WagerRollback WagerKind = "ROLLBACK"
)

type WagerStatus string

const (
	WagerPending          WagerStatus = "PENDING"
	WagerPendingReference WagerStatus = "PENDING_REFERENCE"
	WagerProcessed        WagerStatus = "PROCESSED"
	WagerRejected         WagerStatus = "REJECTED"
	WagerFailed           WagerStatus = "FAILED"
)

type WagerOrigin string

const (
	WagerExternal WagerOrigin = "EXTERNAL"
	WagerInternal WagerOrigin = "INTERNAL"
)

// FailureCode is a stable machine-readable terminal outcome.
type FailureCode string

const (
	FailureInsufficientFunds         FailureCode = "INSUFFICIENT_FUNDS"
	FailureReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
	FailureReferenceNotFound         FailureCode = "REFERENCE_NOT_FOUND"
	FailureReferenceUnsuccessful     FailureCode = "REFERENCE_UNSUCCESSFUL"
	FailureReferenceMismatch         FailureCode = "REFERENCE_MISMATCH"
	FailureAlreadyReversed           FailureCode = "ALREADY_REVERSED"
	FailureBalanceOverflow           FailureCode = "BALANCE_OVERFLOW"
	FailureInfrastructure            FailureCode = "INFRASTRUCTURE_FAILURE"
)

var (
	ErrInvalidWager               = errors.New("invalid wager transaction")
	ErrInvalidWagerKind           = errors.New("invalid wager kind or origin")
	ErrInvalidWagerTransition     = errors.New("invalid wager state transition")
	ErrWagerTerminal              = errors.New("wager transaction is terminal")
	ErrInvalidWagerTime           = errors.New("invalid wager timestamp")
	ErrInvalidWagerResult         = errors.New("invalid wager result")
	ErrInvalidFailureCode         = errors.New("invalid wager failure code")
	ErrWagerReferenceMismatch     = errors.New("wager reference mismatch")
	ErrWagerReferencePending      = errors.New("wager reference is pending")
	ErrWagerReferenceUnsuccessful = errors.New("wager reference was unsuccessful")
	ErrWagerPayloadConflict       = errors.New("wager payload conflict")
)

// WagerRequest contains external business fields and the supplied idempotency key.
// Authentication and provider authorization must run before construction.
type WagerRequest struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	WalletID                       string
	PlayerID                       string
	RoundID                        string
	GameID                         string
	Kind                           WagerKind
	Money                          Money
	ReferenceExternalTransactionID string
}

// WagerState is a detached persistence snapshot. HasResult distinguishes an absent
// result from a valid zero balance. All fields are values, not shared pointers.
type WagerState struct {
	ID                     string
	Origin                 WagerOrigin
	Request                WagerRequest
	PayloadHash            string
	ReferenceTransactionID string
	Status                 WagerStatus
	FailureCode            FailureCode
	HasResult              bool
	ResultBalance          Money
	CreatedAt              time.Time
	UpdatedAt              time.Time
}

// WagerTransaction is immutable. Terminal results cannot be changed by transitions.
// Persistence must enforce idempotency, reference uniqueness, and atomic commits.
type WagerTransaction struct{ state WagerState }

// validWagerText checks opaque identifiers without silently normalizing input.
func validWagerText(value string) bool {
	return value != "" && strings.TrimSpace(value) == value
}

// validateWagerRequest checks external operation metadata, amount, and reference policy.
func validateWagerRequest(r WagerRequest) error {
	for _, value := range []string{r.ProviderID, r.ExternalTransactionID, r.IdempotencyKey, r.WalletID, r.PlayerID, r.RoundID, r.GameID} {
		if !validWagerText(value) {
			return ErrInvalidWager
		}
	}
	if err := r.Money.Validate(); err != nil {
		return err
	}
	if r.Money.minor < 0 {
		return ErrNegativeAmount
	}
	switch r.Kind {
	case WagerLoss:
		if r.Money.minor != 0 {
			return ErrInvalidAmount
		}
	case WagerBet, WagerWin, WagerRefund, WagerRollback:
		if r.Money.minor == 0 {
			return ErrInvalidAmount
		}
	default:
		return ErrInvalidWagerKind
	}
	ref := r.ReferenceExternalTransactionID
	if (r.Kind == WagerRefund || r.Kind == WagerRollback) && ref == "" {
		return ErrWagerReferenceMismatch
	}
	if ref != "" {
		if !validWagerText(ref) || ref == r.ExternalTransactionID {
			return ErrWagerReferenceMismatch
		}
		if r.Kind != WagerWin && r.Kind != WagerRefund && r.Kind != WagerRollback {
			return ErrWagerReferenceMismatch
		}
	}
	return nil
}

// wagerPayloadHash hashes sorted JSON business keys using SHA-256. It excludes
// internal ID, idempotency key, state, timestamps, and transport metadata.
// Amounts are formatted canonically; absent references are represented as "".
func wagerPayloadHash(r WagerRequest) string {
	amount, _ := r.Money.Amount() // Callers validate the request before hashing.
	payload, _ := json.Marshal(map[string]string{
		"providerId": r.ProviderID, "externalTransactionId": r.ExternalTransactionID,
		"walletId": r.WalletID, "playerId": r.PlayerID, "roundId": r.RoundID,
		"gameId": r.GameID, "kind": string(r.Kind), "amount": amount,
		"currency": r.Money.currency, "referenceExternalTransactionId": r.ReferenceExternalTransactionID,
	})
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

// NewWagerTransaction accepts only external kinds and starts in PENDING.
func NewWagerTransaction(id string, r WagerRequest, at time.Time) (WagerTransaction, error) {
	if err := validateWagerRequest(r); err != nil {
		return WagerTransaction{}, err
	}
	return RehydrateWagerTransaction(WagerState{ID: id, Origin: WagerExternal, Request: r,
		PayloadHash: wagerPayloadHash(r), Status: WagerPending, CreatedAt: at, UpdatedAt: at})
}

// NewOpeningTransaction starts an internal positive opening in PENDING, without
// external metadata. The caller supplies a stable ID and commits it with the wallet.
func NewOpeningTransaction(id, walletID, playerID string, amount Money, at time.Time) (WagerTransaction, error) {
	return RehydrateWagerTransaction(WagerState{ID: id, Origin: WagerInternal,
		Request: WagerRequest{WalletID: walletID, PlayerID: playerID, Kind: WagerOpening, Money: amount},
		Status:  WagerPending, CreatedAt: at, UpdatedAt: at})
}

// RehydrateWagerTransaction validates a stored snapshot without applying effects.
func RehydrateWagerTransaction(s WagerState) (WagerTransaction, error) {
	s.CreatedAt = s.CreatedAt.Round(0).UTC()
	s.UpdatedAt = s.UpdatedAt.Round(0).UTC()
	tx := WagerTransaction{state: s}
	if err := tx.Validate(); err != nil {
		return WagerTransaction{}, err
	}
	return tx, nil
}

// Validate checks origin, metadata, hash, state, result, and timestamp consistency.
// Database reference rows must still be checked when loading resolved references.
func (t WagerTransaction) Validate() error {
	s := t.state
	if !validWagerText(s.ID) {
		return ErrInvalidWager
	}
	switch s.Origin {
	case WagerExternal:
		if err := validateWagerRequest(s.Request); err != nil {
			return err
		}
		if s.PayloadHash != wagerPayloadHash(s.Request) {
			return ErrWagerPayloadConflict
		}
	case WagerInternal:
		r := s.Request
		if r.Kind != WagerOpening {
			return ErrInvalidWagerKind
		}
		if !validWagerText(r.WalletID) || !validWagerText(r.PlayerID) {
			return ErrInvalidWager
		}
		if err := r.Money.Validate(); err != nil {
			return err
		}
		if r.Money.minor <= 0 {
			return ErrInvalidAmount
		}
		if r.ProviderID != "" || r.ExternalTransactionID != "" || r.IdempotencyKey != "" || r.RoundID != "" || r.GameID != "" || r.ReferenceExternalTransactionID != "" || s.PayloadHash != "" {
			return ErrInvalidWager
		}
	default:
		return ErrInvalidWagerKind
	}
	if s.CreatedAt.IsZero() || s.UpdatedAt.IsZero() || s.UpdatedAt.Before(s.CreatedAt) {
		return ErrInvalidWagerTime
	}
	if s.ReferenceTransactionID != "" && (!validWagerText(s.ReferenceTransactionID) || s.ReferenceTransactionID == s.ID || s.Request.ReferenceExternalTransactionID == "") {
		return ErrWagerReferenceMismatch
	}
	switch s.Status {
	case WagerPending:
	case WagerPendingReference:
		if s.Request.ReferenceExternalTransactionID == "" || s.ReferenceTransactionID != "" {
			return ErrWagerReferenceMismatch
		}
	case WagerProcessed:
		if !s.HasResult || s.FailureCode != "" {
			return ErrInvalidWagerResult
		}
		if s.Request.ReferenceExternalTransactionID != "" && s.ReferenceTransactionID == "" {
			return ErrWagerReferenceMismatch
		}
	case WagerRejected:
		if !validRejectionCode(s.FailureCode) {
			return ErrInvalidFailureCode
		}
	case WagerFailed:
		if s.FailureCode != FailureInfrastructure {
			return ErrInvalidFailureCode
		}
	default:
		return ErrInvalidWagerTransition
	}
	if s.Status == WagerPending || s.Status == WagerPendingReference {
		if s.HasResult || s.FailureCode != "" {
			return ErrInvalidWagerResult
		}
	}
	if s.HasResult {
		if err := s.Request.Money.sameCurrency(s.ResultBalance); err != nil {
			return err
		}
		if s.ResultBalance.minor < 0 {
			return ErrNegativeBalance
		}
		if s.Request.Kind == WagerOpening && s.Status == WagerProcessed && s.ResultBalance != s.Request.Money {
			return ErrInvalidWagerResult
		}
	} else if s.ResultBalance != (Money{}) {
		return ErrInvalidWagerResult
	}
	return nil
}

// validRejectionCode separates business rejections from permanent infrastructure failures.
func validRejectionCode(code FailureCode) bool {
	switch code {
	case FailureInsufficientFunds, FailureReversalInsufficientFunds, FailureReferenceNotFound,
		FailureReferenceUnsuccessful, FailureReferenceMismatch, FailureAlreadyReversed, FailureBalanceOverflow:
		return true
	}
	return false
}

// Snapshot returns validated state for persistence and replay of the original result.
func (t WagerTransaction) Snapshot() (WagerState, error) {
	if err := t.Validate(); err != nil {
		return WagerState{}, err
	}
	return t.state, nil
}

// transitionState permits changes only to nonterminal transactions at a valid time.
func (t WagerTransaction) transitionState(at time.Time) (WagerState, error) {
	if err := t.Validate(); err != nil {
		return WagerState{}, err
	}
	switch t.state.Status {
	case WagerProcessed, WagerRejected, WagerFailed:
		return WagerState{}, ErrWagerTerminal
	}
	at = at.Round(0).UTC()
	if at.IsZero() || at.Before(t.state.UpdatedAt) {
		return WagerState{}, ErrInvalidWagerTime
	}
	s := t.state
	s.UpdatedAt = at
	return s, nil
}

// WaitForReference marks a still-unresolved dependency. Retry scheduling belongs
// to durable storage; retrying the waiting state does not apply financial effects.
func (t WagerTransaction) WaitForReference(at time.Time) (WagerTransaction, error) {
	s, err := t.transitionState(at)
	if err != nil {
		return WagerTransaction{}, err
	}
	if s.Request.ReferenceExternalTransactionID == "" || s.ReferenceTransactionID != "" {
		return WagerTransaction{}, ErrWagerReferenceMismatch
	}
	s.Status = WagerPendingReference
	return RehydrateWagerTransaction(s)
}

// ResolveReference checks ownership, round, currency, kind, successful status,
// and full reversal amount. It does not claim the reference against other reversals.
func (t WagerTransaction) ResolveReference(reference WagerTransaction, at time.Time) (WagerTransaction, error) {
	s, err := t.transitionState(at)
	if err != nil {
		return WagerTransaction{}, err
	}
	if err := reference.Validate(); err != nil {
		return WagerTransaction{}, err
	}
	r, original := s.Request, reference.state.Request
	if r.ReferenceExternalTransactionID == "" || reference.state.Origin != WagerExternal || s.ID == reference.state.ID ||
		r.ReferenceExternalTransactionID != original.ExternalTransactionID || r.ProviderID != original.ProviderID ||
		r.PlayerID != original.PlayerID || r.WalletID != original.WalletID || r.RoundID != original.RoundID ||
		r.Money.currency != original.Money.currency {
		return WagerTransaction{}, ErrWagerReferenceMismatch
	}
	if s.ReferenceTransactionID != "" && s.ReferenceTransactionID != reference.state.ID {
		return WagerTransaction{}, ErrWagerReferenceMismatch
	}
	switch r.Kind {
	case WagerWin, WagerRefund:
		if original.Kind != WagerBet {
			return WagerTransaction{}, ErrWagerReferenceMismatch
		}
	case WagerRollback:
		if original.Kind != WagerBet && original.Kind != WagerWin && original.Kind != WagerRefund {
			return WagerTransaction{}, ErrWagerReferenceMismatch
		}
	default:
		return WagerTransaction{}, ErrWagerReferenceMismatch
	}
	if (r.Kind == WagerRefund || r.Kind == WagerRollback) && r.Money != original.Money {
		return WagerTransaction{}, ErrWagerReferenceMismatch
	}
	switch reference.state.Status {
	case WagerPending, WagerPendingReference:
		return WagerTransaction{}, ErrWagerReferencePending
	case WagerRejected, WagerFailed:
		return WagerTransaction{}, ErrWagerReferenceUnsuccessful
	}
	s.ReferenceTransactionID = reference.state.ID
	s.Status = WagerPending
	return RehydrateWagerTransaction(s)
}

// MarkProcessed freezes the balance observed by the caller after financial processing.
// The application must atomically persist this state, the wallet, ledger, and events.
func (t WagerTransaction) MarkProcessed(balance Money, at time.Time) (WagerTransaction, error) {
	s, err := t.transitionState(at)
	if err != nil {
		return WagerTransaction{}, err
	}
	s.Status = WagerProcessed
	s.HasResult = true
	s.ResultBalance = balance
	return RehydrateWagerTransaction(s)
}

// Reject records a terminal business failure and an optional observed balance.
// A missing balance is explicit; a zero Money is never treated as a valid balance.
func (t WagerTransaction) Reject(code FailureCode, balance *Money, at time.Time) (WagerTransaction, error) {
	if !validRejectionCode(code) {
		return WagerTransaction{}, ErrInvalidFailureCode
	}
	return t.finishFailure(WagerRejected, code, balance, at)
}

// Fail records only permanent infrastructure failures. Transient failures must
// leave the transaction nonterminal and be retried by the application.
func (t WagerTransaction) Fail(balance *Money, at time.Time) (WagerTransaction, error) {
	return t.finishFailure(WagerFailed, FailureInfrastructure, balance, at)
}

// finishFailure copies optional result values so callers cannot mutate the outcome.
func (t WagerTransaction) finishFailure(status WagerStatus, code FailureCode, balance *Money, at time.Time) (WagerTransaction, error) {
	s, err := t.transitionState(at)
	if err != nil {
		return WagerTransaction{}, err
	}
	s.Status = status
	s.FailureCode = code
	if balance != nil {
		s.HasResult = true
		s.ResultBalance = *balance
	}
	return RehydrateWagerTransaction(s)
}

// CheckPayload compares business content independently of key and transport.
// The application must look up records within the authenticated provider scope.
func (t WagerTransaction) CheckPayload(r WagerRequest) error {
	if err := t.Validate(); err != nil {
		return err
	}
	if err := validateWagerRequest(r); err != nil {
		return err
	}
	if t.state.Origin != WagerExternal || t.state.PayloadHash != wagerPayloadHash(r) {
		return ErrWagerPayloadConflict
	}
	return nil
}
