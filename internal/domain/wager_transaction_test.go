package domain

import (
	"errors"
	"testing"
	"time"
)

// wagerRequest provides a valid external BET request for independent test cases.
func wagerRequest(t *testing.T) WagerRequest {
	return WagerRequest{ProviderID: "provider-a", ExternalTransactionID: "bet-1", IdempotencyKey: "key-1",
		WalletID: "wallet-1", PlayerID: "player-1", RoundID: "round-1", GameID: "game-1",
		Kind: WagerBet, Money: mustMoney(t, 2500, "BRL")}
}

// mustWager creates a transaction fixture and reports invalid test setup immediately.
func mustWager(t *testing.T, id string, r WagerRequest) WagerTransaction {
	t.Helper()
	tx, err := NewWagerTransaction(id, r, walletTime())
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

// processedBet supplies an eligible reference without moving a real wallet.
func processedBet(t *testing.T) WagerTransaction {
	t.Helper()
	tx := mustWager(t, "original", wagerRequest(t))
	done, err := tx.MarkProcessed(mustMoney(t, 7500, "BRL"), walletTime())
	if err != nil {
		t.Fatal(err)
	}
	return done
}

// TestWagerKindsAndInput validates every kind, amount policy, and external metadata.
func TestWagerKindsAndInput(t *testing.T) {
	for _, kind := range []WagerKind{WagerBet, WagerWin, WagerLoss, WagerRefund, WagerRollback, WagerOpening, "UNKNOWN"} {
		for _, minor := range []int64{-1, 0, 1} {
			r := wagerRequest(t)
			r.Kind = kind
			r.Money = mustMoney(t, minor, "BRL")
			if kind == WagerRefund || kind == WagerRollback {
				r.ReferenceExternalTransactionID = "reference"
			}
			tx, err := NewWagerTransaction("id", r, walletTime())
			valid := (kind == WagerLoss && minor == 0) || ((kind == WagerBet || kind == WagerWin || kind == WagerRefund || kind == WagerRollback) && minor > 0)
			if (err == nil) != valid {
				t.Fatalf("%s %d: %v", kind, minor, err)
			}
			if valid && (tx.state.Status != WagerPending || tx.state.CreatedAt.Location() != time.UTC) {
				t.Fatal("invalid initial state")
			}
		}
	}
	changes := []func(*WagerRequest){
		func(r *WagerRequest) { r.ProviderID = "" }, func(r *WagerRequest) { r.ExternalTransactionID = " " },
		func(r *WagerRequest) { r.IdempotencyKey = "" }, func(r *WagerRequest) { r.WalletID = " w" },
		func(r *WagerRequest) { r.PlayerID = "" }, func(r *WagerRequest) { r.RoundID = "" },
		func(r *WagerRequest) { r.GameID = "" }, func(r *WagerRequest) { r.Money = Money{} },
		func(r *WagerRequest) { r.Kind = WagerRefund },
		func(r *WagerRequest) { r.Kind = WagerRollback },
		func(r *WagerRequest) { r.ReferenceExternalTransactionID = "other" },
		func(r *WagerRequest) { r.Kind = WagerWin; r.ReferenceExternalTransactionID = r.ExternalTransactionID },
		func(r *WagerRequest) { r.Kind = WagerWin; r.ReferenceExternalTransactionID = " ref" },
	}
	for i, change := range changes {
		r := wagerRequest(t)
		change(&r)
		if _, err := NewWagerTransaction("id", r, walletTime()); err == nil {
			t.Fatalf("invalid request %d accepted", i)
		}
	}
	if _, err := NewWagerTransaction("", wagerRequest(t), walletTime()); err == nil {
		t.Fatal("empty ID accepted")
	}
	if _, err := NewWagerTransaction("id", wagerRequest(t), time.Time{}); err == nil {
		t.Fatal("empty timestamp accepted")
	}
}

// TestWagerTerminalStates verifies all terminal outcomes reject every mutation.
func TestWagerTerminalStates(t *testing.T) {
	tx := mustWager(t, "id", wagerRequest(t))
	balance := mustMoney(t, 7500, "BRL")
	processed, err := tx.MarkProcessed(balance, walletTime())
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := tx.Reject(FailureInsufficientFunds, &balance, walletTime())
	if err != nil {
		t.Fatal(err)
	}
	failed, err := tx.Fail(nil, walletTime())
	if err != nil {
		t.Fatal(err)
	}
	for _, terminal := range []WagerTransaction{processed, rejected, failed} {
		before := terminal
		calls := []func() (WagerTransaction, error){
			func() (WagerTransaction, error) { return terminal.MarkProcessed(balance, walletTime()) },
			func() (WagerTransaction, error) { return terminal.Reject(FailureBalanceOverflow, nil, walletTime()) },
			func() (WagerTransaction, error) { return terminal.Fail(nil, walletTime()) },
			func() (WagerTransaction, error) { return terminal.WaitForReference(walletTime()) },
			func() (WagerTransaction, error) { return terminal.ResolveReference(processedBet(t), walletTime()) },
		}
		for _, call := range calls {
			if _, err := call(); !errors.Is(err, ErrWagerTerminal) {
				t.Fatal(err)
			}
		}
		if terminal != before {
			t.Fatal("terminal mutated")
		}
		state, err := terminal.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		restored, err := RehydrateWagerTransaction(state)
		if err != nil || restored != terminal {
			t.Fatalf("rehydration: %v", err)
		}
	}
	balance = mustMoney(t, 0, "BRL")
	if rejected.state.ResultBalance.minor != 7500 || processed.state.ResultBalance.minor != 7500 {
		t.Fatal("result was not copied")
	}
	if tx.state.Status != WagerPending {
		t.Fatal("original mutated")
	}
}

// TestWagerReferenceResolution exercises waiting, scope checks, and reference outcomes.
func TestWagerReferenceResolution(t *testing.T) {
	original := processedBet(t)
	for _, kind := range []WagerKind{WagerWin, WagerRefund, WagerRollback} {
		r := wagerRequest(t)
		r.ExternalTransactionID = "reversal"
		r.Kind = kind
		r.ReferenceExternalTransactionID = "bet-1"
		tx := mustWager(t, "reversal", r)
		waiting, err := tx.WaitForReference(walletTime())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := waiting.MarkProcessed(mustMoney(t, 10000, "BRL"), walletTime()); !errors.Is(err, ErrWagerReferenceMismatch) {
			t.Fatal(err)
		}
		repeated, err := waiting.WaitForReference(walletTime().Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		resolved, err := repeated.ResolveReference(original, walletTime().Add(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if resolved.state.ReferenceTransactionID != "original" || resolved.state.Status != WagerPending {
			t.Fatal("reference not resolved")
		}
		if _, err := resolved.MarkProcessed(mustMoney(t, 10000, "BRL"), walletTime().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, err := resolved.WaitForReference(walletTime().Add(time.Second)); err == nil {
			t.Fatal("resolved reference became pending")
		}
		expired, err := waiting.Reject(FailureReferenceNotFound, nil, walletTime())
		if err != nil || expired.state.Status != WagerRejected {
			t.Fatal(err)
		}
	}
	changes := []func(*WagerRequest){
		func(r *WagerRequest) { r.ProviderID = "provider-b" }, func(r *WagerRequest) { r.PlayerID = "player-2" },
		func(r *WagerRequest) { r.WalletID = "wallet-2" }, func(r *WagerRequest) { r.RoundID = "round-2" },
		func(r *WagerRequest) { r.Money = mustMoney(t, 2500, "USD") }, func(r *WagerRequest) { r.Money = mustMoney(t, 2499, "BRL") },
		func(r *WagerRequest) { r.ReferenceExternalTransactionID = "missing" },
	}
	for _, change := range changes {
		r := wagerRequest(t)
		r.Kind = WagerRefund
		r.ExternalTransactionID = "refund"
		r.ReferenceExternalTransactionID = "bet-1"
		change(&r)
		if _, err := mustWager(t, "refund", r).ResolveReference(original, walletTime()); !errors.Is(err, ErrWagerReferenceMismatch) {
			t.Fatal(err)
		}
	}
	r := wagerRequest(t)
	r.Kind = WagerRefund
	r.ExternalTransactionID = "refund"
	r.ReferenceExternalTransactionID = "bet-1"
	refund := mustWager(t, "refund", r)
	pending := mustWager(t, "original", wagerRequest(t))
	if _, err := refund.ResolveReference(pending, walletTime()); !errors.Is(err, ErrWagerReferencePending) {
		t.Fatal(err)
	}
	rejected, _ := pending.Reject(FailureInsufficientFunds, nil, walletTime())
	if _, err := refund.ResolveReference(rejected, walletTime()); !errors.Is(err, ErrWagerReferenceUnsuccessful) {
		t.Fatal(err)
	}
	// WIN can reference a BET with a different amount.
	r.Kind = WagerWin
	r.Money = mustMoney(t, 5000, "BRL")
	if _, err := mustWager(t, "win", r).ResolveReference(original, walletTime()); err != nil {
		t.Fatal(err)
	}
}

// TestWagerOpening verifies internal metadata and exact positive opening results.
func TestWagerOpening(t *testing.T) {
	for _, minor := range []int64{-1, 0, 100} {
		tx, err := NewOpeningTransaction("opening", "wallet-1", "player-1", mustMoney(t, minor, "BRL"), walletTime())
		if minor <= 0 {
			if err == nil {
				t.Fatal("nonpositive opening accepted")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if tx.state.PayloadHash != "" || tx.state.Request.ProviderID != "" {
			t.Fatal("external metadata in opening")
		}
		if _, err := tx.MarkProcessed(mustMoney(t, 99, "BRL"), walletTime()); !errors.Is(err, ErrInvalidWagerResult) {
			t.Fatal(err)
		}
		done, err := tx.MarkProcessed(mustMoney(t, 100, "BRL"), walletTime())
		if err != nil {
			t.Fatal(err)
		}
		if done.state.Status != WagerProcessed {
			t.Fatal("opening not processed")
		}
		state := tx.state
		state.Request.ProviderID = "provider-a"
		if _, err := RehydrateWagerTransaction(state); err == nil {
			t.Fatal("external metadata accepted")
		}
	}
}

// TestWagerPayloadHash checks stable replay content, exclusions, and all business fields.
func TestWagerPayloadHash(t *testing.T) {
	r := wagerRequest(t)
	tx := mustWager(t, "id", r)
	r.IdempotencyKey = "another-key"
	other := mustWager(t, "another-id", r)
	if tx.state.PayloadHash != other.state.PayloadHash || len(tx.state.PayloadHash) != 64 {
		t.Fatal("unstable hash")
	}
	if err := tx.CheckPayload(r); err != nil {
		t.Fatal(err)
	}
	changes := []func(*WagerRequest){
		func(r *WagerRequest) { r.ProviderID = "b" }, func(r *WagerRequest) { r.ExternalTransactionID = "b" },
		func(r *WagerRequest) { r.WalletID = "b" }, func(r *WagerRequest) { r.PlayerID = "b" },
		func(r *WagerRequest) { r.RoundID = "b" }, func(r *WagerRequest) { r.GameID = "b" },
		func(r *WagerRequest) { r.Kind = WagerWin }, func(r *WagerRequest) { r.Money = mustMoney(t, 2600, "BRL") },
		func(r *WagerRequest) { r.Money = mustMoney(t, 2500, "USD") },
	}
	for _, change := range changes {
		request := r
		change(&request)
		if err := tx.CheckPayload(request); !errors.Is(err, ErrWagerPayloadConflict) {
			t.Fatal(err)
		}
	}
	r.Kind = WagerWin
	r.ReferenceExternalTransactionID = "bet-other"
	win := mustWager(t, "win", r)
	r.ReferenceExternalTransactionID = "bet-third"
	if err := win.CheckPayload(r); !errors.Is(err, ErrWagerPayloadConflict) {
		t.Fatal(err)
	}
}

// TestWagerInvalidSnapshotsAndResults rejects corrupted persistence and invalid outcomes.
func TestWagerInvalidSnapshotsAndResults(t *testing.T) {
	tx := mustWager(t, "id", wagerRequest(t))
	changes := []func(*WagerState){
		func(s *WagerState) { s.Origin = "unknown" }, func(s *WagerState) { s.Status = "unknown" },
		func(s *WagerState) { s.PayloadHash = "changed" }, func(s *WagerState) { s.UpdatedAt = s.CreatedAt.Add(-time.Second) },
		func(s *WagerState) { s.Status = WagerProcessed },
		func(s *WagerState) { s.Status = WagerRejected },
		func(s *WagerState) { s.Status = WagerFailed; s.FailureCode = FailureInsufficientFunds },
		func(s *WagerState) { s.Status = WagerPendingReference },
		func(s *WagerState) { s.HasResult = true; s.ResultBalance = mustMoney(t, 0, "BRL") },
		func(s *WagerState) { s.ResultBalance = mustMoney(t, 0, "BRL") },
		func(s *WagerState) { s.FailureCode = FailureInsufficientFunds },
		func(s *WagerState) { s.ReferenceTransactionID = "other" },
	}
	for i, change := range changes {
		s := tx.state
		change(&s)
		if _, err := RehydrateWagerTransaction(s); err == nil {
			t.Fatalf("invalid state %d accepted", i)
		}
	}
	for _, balance := range []Money{{}, mustMoney(t, -1, "BRL"), mustMoney(t, 1, "USD")} {
		if _, err := tx.MarkProcessed(balance, walletTime()); err == nil {
			t.Fatal("invalid balance accepted")
		}
	}
	if _, err := tx.MarkProcessed(mustMoney(t, 0, "BRL"), walletTime()); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.MarkProcessed(mustMoney(t, 0, "BRL"), time.Time{}); !errors.Is(err, ErrInvalidWagerTime) {
		t.Fatal(err)
	}
	if _, err := tx.Reject(FailureInfrastructure, nil, walletTime()); !errors.Is(err, ErrInvalidFailureCode) {
		t.Fatal(err)
	}
	if _, err := tx.WaitForReference(walletTime()); err == nil {
		t.Fatal("BET waiting without reference")
	}
	var zero WagerTransaction
	if _, err := zero.Snapshot(); err == nil {
		t.Fatal("zero snapshot accepted")
	}
	if _, err := zero.Fail(nil, walletTime()); err == nil {
		t.Fatal("zero transition accepted")
	}
}

// TestRollbackKinds verifies supported targets and rejects rollback chains.
func TestRollbackKinds(t *testing.T) {
	bet := processedBet(t)
	for _, kind := range []WagerKind{WagerBet, WagerWin, WagerRefund, WagerRollback, WagerLoss} {
		r := wagerRequest(t)
		r.Kind = kind
		r.ExternalTransactionID = "target"
		if kind == WagerLoss {
			r.Money = mustMoney(t, 0, "BRL")
		}
		if kind == WagerRefund || kind == WagerRollback {
			r.ReferenceExternalTransactionID = "bet-1"
		}
		original := mustWager(t, "target-id", r)
		if r.ReferenceExternalTransactionID != "" {
			var err error
			original, err = original.ResolveReference(bet, walletTime())
			if err != nil {
				t.Fatal(err)
			}
		}
		original, err := original.MarkProcessed(mustMoney(t, 10000, "BRL"), walletTime())
		if err != nil {
			t.Fatal(err)
		}
		request := wagerRequest(t)
		request.Kind = WagerRollback
		request.ExternalTransactionID = "rollback"
		request.ReferenceExternalTransactionID = "target"
		_, err = mustWager(t, "rollback", request).ResolveReference(original, walletTime())
		valid := kind == WagerBet || kind == WagerWin || kind == WagerRefund
		if (err == nil) != valid {
			t.Fatalf("rollback of %s: %v", kind, err)
		}
	}
}
