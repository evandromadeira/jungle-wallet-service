package application

import (
	"context"
	"strings"

	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

// WagerReader scopes every lookup to the authenticated provider in persistence.
type WagerReader interface {
	GetProviderWager(context.Context, string, string, bool) (domain.WagerTransaction, error)
}

// QueryWager reads a committed transaction without changing its processing state.
type QueryWager struct{ reader WagerReader }

// NewQueryWager requires a provider-scoped persistence adapter.
func NewQueryWager(reader WagerReader) (*QueryWager, error) {
	if reader == nil {
		return nil, ErrInvalidWagerCommand
	}
	return &QueryWager{reader}, nil
}

// Execute accepts provider identity only from the authenticated entry point.
// External selects a provider's external transaction ID instead of the internal ID.
func (q *QueryWager) Execute(ctx context.Context, provider, id string, external bool) (domain.WagerState, error) {
	if provider == "" || strings.TrimSpace(provider) != provider || id == "" || strings.TrimSpace(id) != id {
		return domain.WagerState{}, ErrInvalidWagerCommand
	}
	transaction, err := q.reader.GetProviderWager(ctx, provider, id, external)
	if err != nil {
		return domain.WagerState{}, err
	}
	state, err := transaction.Snapshot()
	if err != nil {
		return domain.WagerState{}, err
	}
	if state.Request.ProviderID != provider {
		return domain.WagerState{}, ErrWagerNotFound
	}
	return state, nil
}
