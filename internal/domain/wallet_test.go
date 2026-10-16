package domain

import (
	"errors"
	"math"
	"testing"
	"time"
)

// walletTime provides a deterministic non-UTC clock to verify normalization.
func walletTime() time.Time {
	return time.Date(2026, 10, 13, 9, 0, 0, 0, time.FixedZone("test", -3*60*60))
}

// testWallet creates a valid wallet fixture without hiding construction errors.
func testWallet(t *testing.T, minor int64) Wallet {
	t.Helper()
	wallet, err := NewWallet("wallet-1", "player-1", mustMoney(t, minor, "BRL"), walletTime())
	if err != nil {
		t.Fatal(err)
	}
	return wallet
}

// TestWalletCreationAndRehydration checks initial version and effect-free restoration.
func TestWalletCreationAndRehydration(t *testing.T) {
	for _, minor := range []int64{0, 10000, math.MaxInt64} {
		wallet := testWallet(t, minor)
		state, err := wallet.Snapshot()
		if err != nil {
			t.Fatal(err)
		}
		if state.Version != 1 || state.Balance.minor != minor || state.ID != "wallet-1" || state.PlayerID != "player-1" {
			t.Fatalf("unexpected state: %+v", state)
		}
		if !state.CreatedAt.Equal(walletTime()) || state.CreatedAt.Location() != time.UTC || state.UpdatedAt != state.CreatedAt {
			t.Fatal("timestamps not preserved in UTC")
		}
		state.Version = 42
		state.UpdatedAt = state.CreatedAt.Add(time.Hour)
		restored, err := RehydrateWallet(state)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := restored.Snapshot()
		if err != nil || actual != state {
			t.Fatalf("restoration changed state: %+v, %v", actual, err)
		}
		state.ID = "changed"
		state.Balance = mustMoney(t, 1, "USD")
		original, _ := wallet.Snapshot()
		if original.ID != "wallet-1" || original.Balance.minor != minor || original.Balance.currency != "BRL" {
			t.Fatal("snapshot changed wallet")
		}
	}
}

// TestWalletInvalidState verifies that both construction paths reject broken invariants.
func TestWalletInvalidState(t *testing.T) {
	base, _ := testWallet(t, 0).Snapshot()
	cases := []struct {
		name   string
		change func(*WalletState)
		want   error
	}{
		{"empty wallet ID", func(s *WalletState) { s.ID = "" }, ErrInvalidWalletID},
		{"blank wallet ID", func(s *WalletState) { s.ID = " " }, ErrInvalidWalletID},
		{"padded wallet ID", func(s *WalletState) { s.ID = " w" }, ErrInvalidWalletID},
		{"empty player ID", func(s *WalletState) { s.PlayerID = "" }, ErrInvalidPlayerID},
		{"padded player ID", func(s *WalletState) { s.PlayerID = "p " }, ErrInvalidPlayerID},
		{"invalid money", func(s *WalletState) { s.Balance = Money{} }, ErrInvalidCurrency},
		{"negative balance", func(s *WalletState) { s.Balance = mustMoney(t, -1, "BRL") }, ErrNegativeBalance},
		{"zero version", func(s *WalletState) { s.Version = 0 }, ErrInvalidWalletVersion},
		{"negative version", func(s *WalletState) { s.Version = -1 }, ErrInvalidWalletVersion},
		{"missing creation", func(s *WalletState) { s.CreatedAt = time.Time{} }, ErrInvalidWalletTime},
		{"missing update", func(s *WalletState) { s.UpdatedAt = time.Time{} }, ErrInvalidWalletTime},
		{"backward time", func(s *WalletState) { s.UpdatedAt = s.CreatedAt.Add(-time.Second) }, ErrInvalidWalletTime},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state := base
			tc.change(&state)
			if _, err := RehydrateWallet(state); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
	for _, tc := range []struct {
		id, player string
		balance    Money
		at         time.Time
		want       error
	}{
		{"", "p", base.Balance, walletTime(), ErrInvalidWalletID},
		{"w", "", base.Balance, walletTime(), ErrInvalidPlayerID},
		{"w", "p", Money{}, walletTime(), ErrInvalidCurrency},
		{"w", "p", mustMoney(t, -1, "BRL"), walletTime(), ErrNegativeBalance},
		{"w", "p", base.Balance, time.Time{}, ErrInvalidWalletTime},
	} {
		if _, err := NewWallet(tc.id, tc.player, tc.balance, tc.at); !errors.Is(err, tc.want) {
			t.Fatalf("new: %v", err)
		}
	}
	var zero Wallet
	if !errors.Is(zero.Validate(), ErrInvalidWallet) {
		t.Fatal("zero wallet accepted")
	}
	if _, err := zero.Snapshot(); !errors.Is(err, ErrInvalidWallet) {
		t.Fatal(err)
	}
	for _, op := range []func(Money, time.Time) (Wallet, error){zero.Debit, zero.Credit} {
		if _, err := op(base.Balance, walletTime()); !errors.Is(err, ErrInvalidWallet) {
			t.Fatal(err)
		}
	}
}

// TestWalletMovements checks immutable debit/credit, exact depletion, and version changes.
func TestWalletMovements(t *testing.T) {
	initial := testWallet(t, 10000)
	debited, err := initial.Debit(mustMoney(t, 8000, "BRL"), walletTime().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	state, _ := debited.Snapshot()
	if state.Balance.minor != 2000 || state.Version != 2 || !state.UpdatedAt.Equal(walletTime().Add(time.Second)) {
		t.Fatal(state)
	}
	if initial.state.Balance.minor != 10000 || initial.state.Version != 1 {
		t.Fatal("original mutated")
	}
	if _, err = debited.Debit(mustMoney(t, 8000, "BRL"), walletTime().Add(2*time.Second)); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatal(err)
	}
	empty, err := debited.Debit(mustMoney(t, 2000, "BRL"), walletTime().Add(2*time.Second))
	if err != nil || empty.state.Balance.minor != 0 || empty.state.Version != 3 {
		t.Fatalf("%+v %v", empty, err)
	}
	credited, err := empty.Credit(mustMoney(t, 100, "BRL"), walletTime().Add(3*time.Second))
	if err != nil || credited.state.Balance.minor != 100 || credited.state.Version != 4 || credited.state.CreatedAt != initial.state.CreatedAt {
		t.Fatalf("%+v %v", credited, err)
	}
}

// TestWalletRejectedMovements ensures failures cannot alter balance, version, or timestamps.
func TestWalletRejectedMovements(t *testing.T) {
	wallet := testWallet(t, 100)
	before := wallet
	for _, op := range []func(Money, time.Time) (Wallet, error){wallet.Debit, wallet.Credit} {
		for _, tc := range []struct {
			amount Money
			at     time.Time
			want   error
		}{
			{Money{}, walletTime(), ErrInvalidCurrency},
			{mustMoney(t, 0, "USD"), walletTime(), ErrCurrencyMismatch},
			{mustMoney(t, -1, "BRL"), walletTime(), ErrInvalidMovement},
			{mustMoney(t, 1, "BRL"), time.Time{}, ErrInvalidWalletTime},
			{mustMoney(t, 1, "BRL"), walletTime().Add(-time.Second), ErrInvalidWalletTime},
		} {
			if _, err := op(tc.amount, tc.at); !errors.Is(err, tc.want) {
				t.Fatalf("got %v want %v", err, tc.want)
			}
			if wallet != before {
				t.Fatal("failed operation mutated wallet")
			}
		}
	}
	if _, err := wallet.Debit(mustMoney(t, 101, "BRL"), walletTime()); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatal(err)
	}
	maxWallet := testWallet(t, math.MaxInt64)
	if _, err := maxWallet.Credit(mustMoney(t, 1, "BRL"), walletTime()); !errors.Is(err, ErrMoneyOverflow) {
		t.Fatal(err)
	}
	state := wallet.state
	state.Version = math.MaxInt64
	maxVersion, err := RehydrateWallet(state)
	if err != nil {
		t.Fatal(err)
	}
	for _, op := range []func(Money, time.Time) (Wallet, error){maxVersion.Debit, maxVersion.Credit} {
		if _, err := op(mustMoney(t, 1, "BRL"), walletTime()); !errors.Is(err, ErrWalletVersionOverflow) {
			t.Fatal(err)
		}
	}
}

// TestWalletZeroMovement checks that no-op amounts preserve version and timestamps.
func TestWalletZeroMovement(t *testing.T) {
	for _, version := range []int64{1, math.MaxInt64} {
		state := testWallet(t, 0).state
		state.Version = version
		wallet, err := RehydrateWallet(state)
		if err != nil {
			t.Fatal(err)
		}
		for _, op := range []func(Money, time.Time) (Wallet, error){wallet.Debit, wallet.Credit} {
			got, err := op(mustMoney(t, 0, "BRL"), walletTime().Add(time.Hour))
			if err != nil || got != wallet {
				t.Fatalf("zero movement changed state: %+v %v", got, err)
			}
		}
	}
}
