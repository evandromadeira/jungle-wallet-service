package application

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

var ErrInvalidOpening = errors.New("invalid wallet opening metadata or dependencies")

// OpenWalletCommand is an internal request; entry points must authorize it first.
// IDs are generated internally, not accepted from the external wallet request.
type OpenWalletCommand struct {
	PlayerID       string
	InitialBalance domain.Money
	CorrelationID  string
	CausationID    string
}

// OpenWallet coordinates durable opening without publishing external effects.
// Its dependencies must be safe for concurrent callers; no automatic retry is performed.
type OpenWallet struct {
	work  UnitOfWork
	newID func() (string, error)
	now   func() time.Time
}

// NewOpenWallet injects persistence, an ID generator and a clock for deterministic tests.
func NewOpenWallet(work UnitOfWork, newID func() (string, error), now func() time.Time) (*OpenWallet, error) {
	if work == nil || newID == nil || now == nil {
		return nil, ErrInvalidOpening
	}
	return &OpenWallet{work: work, newID: newID, now: now}, nil
}

// Execute returns a wallet only after commit. Errors return an invalid zero wallet;
// an ambiguous commit failure must be reconciled, never blindly retried.
func (u *OpenWallet) Execute(ctx context.Context, c OpenWalletCommand) (domain.Wallet, error) {
	if err := ctx.Err(); err != nil {
		return domain.Wallet{}, err
	}
	if c.CorrelationID == "" || strings.TrimSpace(c.CorrelationID) != c.CorrelationID ||
		(c.CausationID != "" && strings.TrimSpace(c.CausationID) != c.CausationID) {
		return domain.Wallet{}, ErrInvalidOpening
	}
	id, err := u.newID()
	if err != nil {
		return domain.Wallet{}, err
	}
	at := u.now().Round(0).UTC().Truncate(time.Microsecond)
	wallet, err := domain.NewWallet(id, c.PlayerID, c.InitialBalance, at)
	if err != nil {
		return domain.Wallet{}, err
	}
	zero, err := c.InitialBalance.IsZero()
	if err != nil {
		return domain.Wallet{}, err
	}
	var tx domain.WagerTransaction
	var entry domain.WalletLedgerEntry
	var processed domain.WagerTransactionProcessed
	var changed domain.WalletBalanceChanged
	if !zero {
		ids := make([]string, 4)
		seen := map[string]bool{id: true}
		for i := range ids {
			ids[i], err = u.newID()
			if err != nil {
				return domain.Wallet{}, err
			}
			if ids[i] == "" || strings.TrimSpace(ids[i]) != ids[i] || seen[ids[i]] {
				return domain.Wallet{}, ErrInvalidOpening
			}
			seen[ids[i]] = true
		}
		tx, err = domain.NewOpeningTransaction(ids[0], id, c.PlayerID, c.InitialBalance, at)
		if err != nil {
			return domain.Wallet{}, err
		}
		tx, err = tx.MarkProcessed(c.InitialBalance, at)
		if err != nil {
			return domain.Wallet{}, err
		}
		currency, _ := c.InitialBalance.Currency()
		before, err := domain.ZeroMoney(currency)
		if err != nil {
			return domain.Wallet{}, err
		}
		entry, err = domain.NewWalletLedgerEntry(ids[1], tx, domain.LedgerCredit, before, c.InitialBalance, at)
		if err != nil {
			return domain.Wallet{}, err
		}
		processed, err = domain.NewWagerTransactionProcessed(domain.EventMetadata{EventID: ids[2], CorrelationID: c.CorrelationID, CausationID: c.CausationID}, tx)
		if err != nil {
			return domain.Wallet{}, err
		}
		changed, err = domain.NewWalletBalanceChanged(domain.EventMetadata{EventID: ids[3], CorrelationID: c.CorrelationID, CausationID: c.CausationID}, tx, entry, wallet)
		if err != nil {
			return domain.Wallet{}, err
		}
	}
	err = u.work.WithinTransaction(ctx, func(r Repositories) error {
		if e := r.Wallets.Create(ctx, wallet); e != nil {
			return e
		}
		if zero {
			return nil
		}
		inserted, e := r.Wagers.Create(ctx, tx)
		if e != nil {
			return e
		}
		if !inserted {
			return ErrWagerConflict
		}
		if e = r.Ledger.Append(ctx, entry); e != nil {
			return e
		}
		if e = r.Outbox.Append(ctx, processed); e != nil {
			return e
		}
		return r.Outbox.Append(ctx, changed)
	})
	if err != nil {
		return domain.Wallet{}, err
	}
	return wallet, nil
}
