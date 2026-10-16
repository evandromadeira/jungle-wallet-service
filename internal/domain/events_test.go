package domain

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// eventValue stops a test at an unexpected construction failure.
func eventValue[T any](t *testing.T, value T, err error) T {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	return value
}

// TestOpeningEvents verifies public JSON, detached snapshots and wallet linkage.
func TestOpeningEvents(t *testing.T) {
	at := time.Date(2026, 10, 14, 10, 0, 0, 0, time.FixedZone("local", -3*3600))
	amount, err := NewMoney(100, "BRL")
	amount = eventValue(t, amount, err)
	tx, err := NewOpeningTransaction("opening", "w", "p", amount, at)
	tx = eventValue(t, tx, err)
	tx, err = tx.MarkProcessed(amount, at)
	tx = eventValue(t, tx, err)
	m := EventMetadata{EventID: "event", CorrelationID: "correlation"}
	processed, err := NewWagerTransactionProcessed(m, tx)
	processed = eventValue(t, processed, err)
	raw, err := json.Marshal(processed)
	raw = eventValue(t, raw, err)
	var envelope map[string]json.RawMessage
	if err = json.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"eventId", "eventType", "aggregateId", "correlationId", "occurredAt", "version", "data"} {
		if _, ok := envelope[key]; !ok {
			t.Fatalf("missing %s: %s", key, raw)
		}
	}
	if string(envelope["eventType"]) != `"WagerTransactionProcessed"` || string(envelope["version"]) != "1" || string(envelope["occurredAt"]) != `"2026-10-14T13:00:00Z"` {
		t.Fatalf("invalid envelope: %s", raw)
	}
	if strings.Contains(string(raw), "providerId") || strings.Contains(string(raw), "causationId") || !strings.Contains(string(raw), `"amount":"1.00"`) {
		t.Fatalf("invalid opening: %s", raw)
	}
	snapshot, err := processed.Snapshot()
	snapshot = eventValue(t, snapshot, err)
	snapshot.Payload = "{}"
	snapshot.EventID = "changed"
	again, err := json.Marshal(processed)
	again = eventValue(t, again, err)
	if string(again) != string(raw) {
		t.Fatal("snapshot mutation changed event")
	}
	zero, err := ZeroMoney("BRL")
	zero = eventValue(t, zero, err)
	entry, err := NewWalletLedgerEntry("entry", tx, LedgerCredit, zero, amount, at)
	entry = eventValue(t, entry, err)
	wallet, err := NewWallet("w", "p", amount, at)
	wallet = eventValue(t, wallet, err)
	changed, err := NewWalletBalanceChanged(m, tx, entry, wallet)
	changed = eventValue(t, changed, err)
	data, err := json.Marshal(changed)
	data = eventValue(t, data, err)
	for _, key := range []string{"walletId", "transactionId", "direction", "money", "balanceBefore", "balanceAfter", "walletVersion"} {
		if !strings.Contains(string(data), `"`+key+`":`) {
			t.Fatalf("missing %s", key)
		}
	}
	wrong, err := NewWallet("other", "p", amount, at)
	wrong = eventValue(t, wrong, err)
	if _, err = NewWalletBalanceChanged(m, tx, entry, wrong); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("wallet mismatch: %v", err)
	}
	if _, err = NewWagerTransactionProcessed(EventMetadata{}, tx); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("missing metadata: %v", err)
	}
	if _, err = json.Marshal(WagerTransactionProcessed{}); !errors.Is(err, ErrInvalidEvent) {
		t.Fatalf("zero event: %v", err)
	}
}

// TestWagerEvents checks status gates, LOSS and external reference metadata.
func TestWagerEvents(t *testing.T) {
	pending := mustWager(t, "t", wagerRequest(t))
	state, err := pending.Snapshot()
	state = eventValue(t, state, err)
	m := EventMetadata{"e", "c", "cause"}
	if _, err = NewWagerTransactionProcessed(m, pending); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal(err)
	}
	if _, err = NewWagerTransactionRejected(m, pending); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal(err)
	}
	if _, err = NewWagerTransactionPendingReference(m, pending); !errors.Is(err, ErrInvalidEvent) {
		t.Fatal(err)
	}
	rejected, err := pending.Reject(FailureInsufficientFunds, nil, state.UpdatedAt)
	rejected = eventValue(t, rejected, err)
	rejection, err := NewWagerTransactionRejected(m, rejected)
	rejection = eventValue(t, rejection, err)
	raw, err := json.Marshal(rejection)
	raw = eventValue(t, raw, err)
	if !strings.Contains(string(raw), `"failureCode":"INSUFFICIENT_FUNDS"`) || !strings.Contains(string(raw), `"causationId":"cause"`) {
		t.Fatal(string(raw))
	}
	r := wagerRequest(t)
	r.Kind = WagerRefund
	r.ReferenceExternalTransactionID = "original"
	waiting := mustWager(t, "ref", r)
	waiting, err = waiting.WaitForReference(state.UpdatedAt)
	waiting = eventValue(t, waiting, err)
	event, err := NewWagerTransactionPendingReference(m, waiting)
	event = eventValue(t, event, err)
	raw, err = json.Marshal(event)
	raw = eventValue(t, raw, err)
	if !strings.Contains(string(raw), `"referenceExternalTransactionId":"original"`) {
		t.Fatal(string(raw))
	}
	r = wagerRequest(t)
	r.Kind = WagerLoss
	r.Money, err = ZeroMoney("BRL")
	r.Money = eventValue(t, r.Money, err)
	loss := mustWager(t, "loss", r)
	loss, err = loss.MarkProcessed(r.Money, state.UpdatedAt)
	loss = eventValue(t, loss, err)
	if _, err = NewWagerTransactionProcessed(m, loss); err != nil {
		t.Fatal(err)
	}
	if _, err = NewWalletBalanceChanged(m, loss, WalletLedgerEntry{}, Wallet{}); err == nil {
		t.Fatal("LOSS accepted without movement")
	}
	for _, metadata := range []EventMetadata{{" e", "c", ""}, {"e", " ", ""}, {"e", "c", " "}} {
		if _, err = NewWagerTransactionProcessed(metadata, loss); !errors.Is(err, ErrInvalidEvent) {
			t.Fatal(err)
		}
	}
}
