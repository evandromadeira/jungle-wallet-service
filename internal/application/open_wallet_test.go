package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

type openingWork struct {
	calls       int
	wallet      openingWallet
	commitError error
}

// WithinTransaction intentionally supplies only Wallets to detect extra writes for zero.
func (w *openingWork) WithinTransaction(ctx context.Context, fn func(Repositories) error) error {
	w.calls++
	if err := fn(Repositories{Wallets: &w.wallet}); err != nil {
		return err
	}
	return w.commitError
}

type openingWallet struct {
	WalletRepository
	calls int
}

// Create records only the operation expected in a zero-balance opening.
func (w *openingWallet) Create(context.Context, domain.Wallet) error { w.calls++; return nil }

// TestOpenWalletZeroAndFailures checks no financial writes and no successful result on commit failure.
func TestOpenWalletZeroAndFailures(t *testing.T) {
	work := &openingWork{}
	now := func() time.Time { return time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC) }
	service, err := NewOpenWallet(work, func() (string, error) { return "wallet", nil }, now)
	if err != nil {
		t.Fatal(err)
	}
	zero, _ := domain.ZeroMoney("BRL")
	c := OpenWalletCommand{PlayerID: "p", InitialBalance: zero, CorrelationID: "c"}
	if _, err = service.Execute(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if work.calls != 1 || work.wallet.calls != 1 {
		t.Fatal("unexpected zero opening writes")
	}
	marker := errors.New("commit outcome unknown")
	work.commitError = marker
	result, err := service.Execute(context.Background(), c)
	if !errors.Is(err, marker) || result != (domain.Wallet{}) || work.calls != 2 {
		t.Fatal("commit error hidden or retried")
	}
	c.CorrelationID = " "
	if _, err = service.Execute(context.Background(), c); !errors.Is(err, ErrInvalidOpening) || work.calls != 2 {
		t.Fatal("invalid metadata reached persistence")
	}
	if _, err = NewOpenWallet(nil, nil, nil); !errors.Is(err, ErrInvalidOpening) {
		t.Fatal(err)
	}
}
