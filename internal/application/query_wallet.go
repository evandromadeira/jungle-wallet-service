package application

import (
	"context"
	"errors"
	"strings"

	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

var ErrInvalidWalletQuery = errors.New("invalid wallet query or dependencies")

// WalletReader loads a committed wallet without acquiring a write lock.
type WalletReader interface {
	GetWallet(context.Context, string) (domain.Wallet, error)
}

// QueryWallet exposes current wallet state to trusted internal entry points.
type QueryWallet struct{ reader WalletReader }

// NewQueryWallet injects the committed wallet reader.
func NewQueryWallet(reader WalletReader) (*QueryWallet, error) {
	if reader == nil {
		return nil, ErrInvalidWalletQuery
	}
	return &QueryWallet{reader}, nil
}

// Execute validates the opaque identity and returns the persisted snapshot.
// The entry point must authorize the internal service before calling Execute.
func (q *QueryWallet) Execute(ctx context.Context, id string) (domain.WalletState, error) {
	if id == "" || strings.TrimSpace(id) != id {
		return domain.WalletState{}, ErrInvalidWalletQuery
	}
	wallet, err := q.reader.GetWallet(ctx, id)
	if err != nil {
		return domain.WalletState{}, err
	}
	return wallet.Snapshot()
}
