package domain

import (
	"errors"
	"math"
	"testing"
	"time"
)

// ledgerState provides a valid debit snapshot for invariant tests.
func ledgerState(t *testing.T) WalletLedgerState {
	return WalletLedgerState{ID: "entry", WalletID: "wallet-1", TransactionID: "original",
		Direction: LedgerDebit, Money: mustMoney(t, 2500, "BRL"), BalanceBefore: mustMoney(t, 10000, "BRL"),
		BalanceAfter: mustMoney(t, 7500, "BRL"), CreatedAt: walletTime()}
}

// TestLedgerCreationAndSnapshot checks transaction linkage and immutable restoration.
func TestLedgerCreationAndSnapshot(t *testing.T) {
	s := ledgerState(t)
	entry, err := NewWalletLedgerEntry(s.ID, processedBet(t), s.Direction, s.BalanceBefore, s.BalanceAfter, s.CreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := entry.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.WalletID != s.WalletID || snapshot.TransactionID != s.TransactionID || snapshot.CreatedAt.Location() != time.UTC {
		t.Fatal(snapshot)
	}
	restored, err := RehydrateWalletLedgerEntry(snapshot)
	if err != nil || restored != entry {
		t.Fatal(err)
	}
	snapshot.ID = "changed"
	snapshot.Money = mustMoney(t, 1, "USD")
	if entry.state.ID != "entry" || entry.state.Money != s.Money {
		t.Fatal("entry mutated")
	}
	var zero WalletLedgerEntry
	if _, err := zero.Snapshot(); !errors.Is(err, ErrInvalidLedgerEntry) {
		t.Fatal(err)
	}
}

// TestLedgerInvalidSnapshots checks all persisted financial invariants.
func TestLedgerInvalidSnapshots(t *testing.T) {
	cases := []struct {
		change func(*WalletLedgerState)
		want   error
	}{
		{func(s *WalletLedgerState) { s.ID = "" }, ErrInvalidLedgerEntry},
		{func(s *WalletLedgerState) { s.WalletID = " " }, ErrInvalidLedgerEntry},
		{func(s *WalletLedgerState) { s.TransactionID = " x" }, ErrInvalidLedgerEntry},
		{func(s *WalletLedgerState) { s.CreatedAt = time.Time{} }, ErrInvalidLedgerEntry},
		{func(s *WalletLedgerState) { s.Money = Money{} }, ErrInvalidCurrency},
		{func(s *WalletLedgerState) { s.BalanceBefore = Money{} }, ErrInvalidCurrency},
		{func(s *WalletLedgerState) { s.BalanceAfter = Money{} }, ErrInvalidCurrency},
		{func(s *WalletLedgerState) { s.BalanceBefore = mustMoney(t, 10000, "USD") }, ErrCurrencyMismatch},
		{func(s *WalletLedgerState) { s.BalanceAfter = mustMoney(t, 7500, "USD") }, ErrCurrencyMismatch},
		{func(s *WalletLedgerState) { s.Money = mustMoney(t, 0, "BRL") }, ErrInvalidAmount},
		{func(s *WalletLedgerState) { s.Money = mustMoney(t, -1, "BRL") }, ErrInvalidAmount},
		{func(s *WalletLedgerState) { s.BalanceBefore = mustMoney(t, -1, "BRL") }, ErrNegativeBalance},
		{func(s *WalletLedgerState) { s.BalanceAfter = mustMoney(t, -1, "BRL") }, ErrNegativeBalance},
		{func(s *WalletLedgerState) { s.Direction = "unknown" }, ErrInvalidLedgerDirection},
		{func(s *WalletLedgerState) { s.BalanceAfter = mustMoney(t, 7501, "BRL") }, ErrLedgerBalanceMismatch},
		{func(s *WalletLedgerState) { s.Money = mustMoney(t, 10001, "BRL") }, ErrInsufficientFunds},
		{func(s *WalletLedgerState) {
			s.Direction = LedgerCredit
			s.BalanceBefore = mustMoney(t, math.MaxInt64, "BRL")
		}, ErrMoneyOverflow},
	}
	for i, tc := range cases {
		s := ledgerState(t)
		tc.change(&s)
		if _, err := RehydrateWalletLedgerEntry(s); !errors.Is(err, tc.want) {
			t.Fatalf("case %d: %v, want %v", i, err, tc.want)
		}
	}
	for _, direction := range []LedgerDirection{LedgerDebit, LedgerCredit} {
		s := ledgerState(t)
		s.Direction = direction
		s.Money = mustMoney(t, math.MaxInt64, "BRL")
		if direction == LedgerDebit {
			s.BalanceBefore = s.Money
			s.BalanceAfter = mustMoney(t, 0, "BRL")
		} else {
			s.BalanceBefore = mustMoney(t, 0, "BRL")
			s.BalanceAfter = s.Money
		}
		if _, err := RehydrateWalletLedgerEntry(s); err != nil {
			t.Fatal(err)
		}
	}
}

// TestLedgerRejectsNonfinancialTransactions prevents LOSS and nonprocessed entries.
func TestLedgerRejectsNonfinancialTransactions(t *testing.T) {
	r := wagerRequest(t)
	pending := mustWager(t, "pending", r)
	rejected, _ := pending.Reject(FailureInsufficientFunds, nil, walletTime())
	failed, _ := pending.Fail(nil, walletTime())
	r.Kind = WagerLoss
	r.Money = mustMoney(t, 0, "BRL")
	loss := mustWager(t, "loss", r)
	loss, _ = loss.MarkProcessed(mustMoney(t, 10000, "BRL"), walletTime())
	for _, tx := range []WagerTransaction{{}, pending, rejected, failed, loss} {
		if _, err := NewWalletLedgerEntry("entry", tx, LedgerDebit, mustMoney(t, 10000, "BRL"), mustMoney(t, 7500, "BRL"), walletTime()); err == nil {
			t.Fatal("invalid transaction accepted")
		}
	}
	bet := processedBet(t)
	for _, direction := range []LedgerDirection{LedgerCredit, "unknown"} {
		if _, err := NewWalletLedgerEntry("entry", bet, direction, mustMoney(t, 10000, "BRL"), mustMoney(t, 7500, "BRL"), walletTime()); !errors.Is(err, ErrInvalidLedgerDirection) {
			t.Fatal(err)
		}
	}
	if _, err := NewWalletLedgerEntry("entry", bet, LedgerDebit, mustMoney(t, 10000, "BRL"), mustMoney(t, 7000, "BRL"), walletTime()); !errors.Is(err, ErrLedgerTransactionMismatch) {
		t.Fatal(err)
	}
	if _, err := NewWalletLedgerEntry("entry", bet, LedgerDebit, mustMoney(t, 10000, "BRL"), mustMoney(t, 7500, "BRL"), walletTime().Add(-time.Second)); !errors.Is(err, ErrLedgerTransactionMismatch) {
		t.Fatal(err)
	}
}

// TestLedgerCredits checks WIN, REFUND, and positive OPENING entries.
func TestLedgerCredits(t *testing.T) {
	for _, kind := range []WagerKind{WagerWin, WagerRefund, WagerOpening} {
		amount := mustMoney(t, 2500, "BRL")
		before := mustMoney(t, 7500, "BRL")
		after := mustMoney(t, 10000, "BRL")
		var tx WagerTransaction
		var err error
		if kind == WagerOpening {
			before = mustMoney(t, 0, "BRL")
			after = amount
			tx, err = NewOpeningTransaction("open", "wallet-1", "player-1", amount, walletTime())
		} else {
			r := wagerRequest(t)
			r.Kind = kind
			r.ExternalTransactionID = "credit"
			if kind == WagerRefund {
				r.ReferenceExternalTransactionID = "bet-1"
			}
			tx = mustWager(t, "credit", r)
			if kind == WagerRefund {
				tx, err = tx.ResolveReference(processedBet(t), walletTime())
			}
		}
		if err != nil {
			t.Fatal(err)
		}
		tx, err = tx.MarkProcessed(after, walletTime())
		if err != nil {
			t.Fatal(err)
		}
		if _, err = NewWalletLedgerEntry("entry", tx, LedgerCredit, before, after, walletTime()); err != nil {
			t.Fatal(err)
		}
		if _, err = NewWalletLedgerEntry("entry", tx, LedgerDebit, before, after, walletTime()); !errors.Is(err, ErrInvalidLedgerDirection) {
			t.Fatal(err)
		}
		if kind == WagerOpening {
			if _, err = NewWalletLedgerEntry("entry", tx, LedgerCredit, amount, after, walletTime()); !errors.Is(err, ErrLedgerBalanceMismatch) {
				t.Fatal(err)
			}
		}
	}
}

// TestLedgerRollbackDirections verifies both arithmetic directions of reversals.
func TestLedgerRollbackDirections(t *testing.T) {
	for _, kind := range []WagerKind{WagerBet, WagerWin} {
		r := wagerRequest(t)
		r.Kind = kind
		original := mustWager(t, "original", r)
		original, _ = original.MarkProcessed(mustMoney(t, 7500, "BRL"), walletTime())
		r.Kind = WagerRollback
		r.ExternalTransactionID = "rollback"
		r.ReferenceExternalTransactionID = "bet-1"
		tx := mustWager(t, "rollback", r)
		tx, err := tx.ResolveReference(original, walletTime())
		if err != nil {
			t.Fatal(err)
		}
		before := mustMoney(t, 7500, "BRL")
		after := mustMoney(t, 10000, "BRL")
		direction := LedgerCredit
		if kind == WagerWin {
			after = mustMoney(t, 5000, "BRL")
			direction = LedgerDebit
		}
		tx, err = tx.MarkProcessed(after, walletTime())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := NewWalletLedgerEntry("entry", tx, direction, before, after, walletTime()); err != nil {
			t.Fatal(err)
		}
	}
}
