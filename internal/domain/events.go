package domain

import (
	"encoding/json"
	"errors"
	"time"
)

var ErrInvalidEvent = errors.New("invalid event or incompatible source state")

// EventMetadata supplies stable identifiers; retries must reuse the stored event.
type EventMetadata struct{ EventID, CorrelationID, CausationID string }

// EventSnapshot is a detached outbox record. Payload contains only the data object.
type EventSnapshot struct {
	EventID       string    `json:"eventId"`
	EventType     string    `json:"eventType"`
	AggregateID   string    `json:"aggregateId"`
	CorrelationID string    `json:"correlationId"`
	CausationID   string    `json:"causationId,omitempty"`
	OccurredAt    time.Time `json:"occurredAt"`
	Version       int       `json:"version"`
	Payload       string    `json:"-"`
}

type event struct{ snapshot EventSnapshot }

// Snapshot returns a detached immutable-data representation for outbox persistence.
func (e event) Snapshot() (EventSnapshot, error) {
	if e.snapshot.EventID == "" {
		return EventSnapshot{}, ErrInvalidEvent
	}
	return e.snapshot, nil
}

// MarshalJSON emits the envelope with typed constructor data as a JSON object.
func (e event) MarshalJSON() ([]byte, error) {
	s, err := e.Snapshot()
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		EventSnapshot
		Data json.RawMessage `json:"data"`
	}{s, json.RawMessage(s.Payload)})
}

// newEvent freezes validated typed data and sets the event contract version.
func newEvent(m EventMetadata, kind, aggregate string, at time.Time, data any) (event, error) {
	if !validWagerText(m.EventID) || !validWagerText(m.CorrelationID) ||
		(m.CausationID != "" && !validWagerText(m.CausationID)) || !validWagerText(aggregate) ||
		at.IsZero() || at.Year() < 1 || at.Year() > 9999 {
		return event{}, ErrInvalidEvent
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return event{}, err
	}
	return event{EventSnapshot{m.EventID, kind, aggregate, m.CorrelationID, m.CausationID, at.Round(0).UTC(), 1, string(payload)}}, nil
}

// WagerEventData contains business identity; internal OPENING omits external metadata.
type WagerEventData struct {
	TransactionID                  string      `json:"transactionId"`
	WalletID                       string      `json:"walletId"`
	PlayerID                       string      `json:"playerId"`
	Origin                         WagerOrigin `json:"origin"`
	Kind                           WagerKind   `json:"kind"`
	Money                          Money       `json:"money"`
	ProviderID                     string      `json:"providerId,omitempty"`
	ExternalTransactionID          string      `json:"externalTransactionId,omitempty"`
	RoundID                        string      `json:"roundId,omitempty"`
	GameID                         string      `json:"gameId,omitempty"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId,omitempty"`
}
type WagerProcessedData struct {
	WagerEventData
	Balance Money `json:"balance"`
}
type WagerRejectedData struct {
	WagerEventData
	FailureCode FailureCode `json:"failureCode"`
}
type WagerPendingReferenceData struct{ WagerEventData }
type WalletBalanceChangedData struct {
	WalletID      string          `json:"walletId"`
	TransactionID string          `json:"transactionId"`
	Direction     LedgerDirection `json:"direction"`
	Money         Money           `json:"money"`
	BalanceBefore Money           `json:"balanceBefore"`
	BalanceAfter  Money           `json:"balanceAfter"`
	WalletVersion int64           `json:"walletVersion"`
}

type WagerTransactionProcessed struct{ event }
type WagerTransactionRejected struct{ event }
type WagerTransactionPendingReference struct{ event }
type WalletBalanceChanged struct{ event }

// wagerEventData copies validated business fields without exposing mutable state.
func wagerEventData(s WagerState) WagerEventData {
	r := s.Request
	return WagerEventData{s.ID, r.WalletID, r.PlayerID, s.Origin, r.Kind, r.Money,
		r.ProviderID, r.ExternalTransactionID, r.RoundID, r.GameID, r.ReferenceExternalTransactionID}
}

// NewWagerTransactionProcessed captures a processed result, including LOSS and OPENING.
func NewWagerTransactionProcessed(m EventMetadata, tx WagerTransaction) (WagerTransactionProcessed, error) {
	s, err := tx.Snapshot()
	if err != nil {
		return WagerTransactionProcessed{}, err
	}
	if s.Status != WagerProcessed {
		return WagerTransactionProcessed{}, ErrInvalidEvent
	}
	e, err := newEvent(m, "WagerTransactionProcessed", s.ID, s.UpdatedAt, WagerProcessedData{wagerEventData(s), s.ResultBalance})
	return WagerTransactionProcessed{e}, err
}

// NewWagerTransactionRejected captures a terminal business rejection and its code.
func NewWagerTransactionRejected(m EventMetadata, tx WagerTransaction) (WagerTransactionRejected, error) {
	s, err := tx.Snapshot()
	if err != nil {
		return WagerTransactionRejected{}, err
	}
	if s.Status != WagerRejected {
		return WagerTransactionRejected{}, ErrInvalidEvent
	}
	e, err := newEvent(m, "WagerTransactionRejected", s.ID, s.UpdatedAt, WagerRejectedData{wagerEventData(s), s.FailureCode})
	return WagerTransactionRejected{e}, err
}

// NewWagerTransactionPendingReference captures a durable wait for an unresolved reference.
func NewWagerTransactionPendingReference(m EventMetadata, tx WagerTransaction) (WagerTransactionPendingReference, error) {
	s, err := tx.Snapshot()
	if err != nil {
		return WagerTransactionPendingReference{}, err
	}
	if s.Status != WagerPendingReference {
		return WagerTransactionPendingReference{}, ErrInvalidEvent
	}
	e, err := newEvent(m, "WagerTransactionPendingReference", s.ID, s.UpdatedAt, WagerPendingReferenceData{wagerEventData(s)})
	return WagerTransactionPendingReference{e}, err
}

// NewWalletBalanceChanged checks the ledger against the processed transaction and
// resulting wallet. The repository must still prove atomicity and version progression.
func NewWalletBalanceChanged(m EventMetadata, tx WagerTransaction, entry WalletLedgerEntry, wallet Wallet) (WalletBalanceChanged, error) {
	t, err := tx.Snapshot()
	if err != nil {
		return WalletBalanceChanged{}, err
	}
	l, err := entry.Snapshot()
	if err != nil {
		return WalletBalanceChanged{}, err
	}
	w, err := wallet.Snapshot()
	if err != nil {
		return WalletBalanceChanged{}, err
	}
	if l.TransactionID != t.ID || l.WalletID != w.ID || w.ID != t.Request.WalletID ||
		w.PlayerID != t.Request.PlayerID || l.Money != t.Request.Money || w.Balance != l.BalanceAfter ||
		w.UpdatedAt.Before(t.UpdatedAt) || l.CreatedAt.Before(w.UpdatedAt) ||
		(t.Request.Kind == WagerOpening && w.Version != 1) {
		return WalletBalanceChanged{}, ErrInvalidEvent
	}
	if _, err = NewWalletLedgerEntry(l.ID, tx, l.Direction, l.BalanceBefore, l.BalanceAfter, l.CreatedAt); err != nil {
		return WalletBalanceChanged{}, err
	}
	e, err := newEvent(m, "WalletBalanceChanged", w.ID, l.CreatedAt, WalletBalanceChangedData{
		w.ID, t.ID, l.Direction, l.Money, l.BalanceBefore, l.BalanceAfter, w.Version})
	return WalletBalanceChanged{e}, err
}
