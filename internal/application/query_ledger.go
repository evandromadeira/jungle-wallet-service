package application

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

var ErrInvalidLedgerQuery = errors.New("invalid ledger query")

// LedgerPosition is an exclusive keyset boundary, scoped to one wallet.
type LedgerPosition struct {
	WalletID  string
	ID        string
	CreatedAt time.Time
}

// LedgerReader returns at most limit entries in ascending (created_at, id) order.
type LedgerReader interface {
	ReadLedger(context.Context, string, *LedgerPosition, int) ([]domain.WalletLedgerState, error)
}

// LedgerPage contains immutable entries and an opaque continuation token.
type LedgerPage struct {
	Entries    []domain.WalletLedgerState
	NextCursor string
}

// QueryLedger validates pagination independently from the HTTP transport.
type QueryLedger struct{ reader LedgerReader }

// NewQueryLedger injects the persistence reader without owning its resources.
func NewQueryLedger(reader LedgerReader) (*QueryLedger, error) {
	if reader == nil {
		return nil, ErrInvalidLedgerQuery
	}
	return &QueryLedger{reader}, nil
}

// Execute uses keyset pagination; concurrent inserts do not provide a frozen snapshot.
func (q *QueryLedger) Execute(ctx context.Context, wallet, cursor string, limit int) (LedgerPage, error) {
	if wallet == "" || strings.TrimSpace(wallet) != wallet || limit < 1 || limit > 100 || len(cursor) > 4096 {
		return LedgerPage{}, ErrInvalidLedgerQuery
	}
	var position *LedgerPosition
	if cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(cursor)
		if err != nil {
			return LedgerPage{}, ErrInvalidLedgerQuery
		}
		var p LedgerPosition
		if json.Unmarshal(raw, &p) != nil || p.WalletID != wallet || p.ID == "" || p.CreatedAt.IsZero() {
			return LedgerPage{}, ErrInvalidLedgerQuery
		}
		position = &p
	}
	entries, err := q.reader.ReadLedger(ctx, wallet, position, limit+1)
	if err != nil {
		return LedgerPage{}, err
	}
	page := LedgerPage{Entries: entries}
	if len(entries) > limit {
		page.Entries = entries[:limit]
		last := page.Entries[limit-1]
		raw, err := json.Marshal(LedgerPosition{wallet, last.ID, last.CreatedAt})
		if err != nil {
			return LedgerPage{}, err
		}
		page.NextCursor = base64.RawURLEncoding.EncodeToString(raw)
	}
	return page, nil
}
