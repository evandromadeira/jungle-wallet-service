//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

// TestFinancialRepositories verifies four adapters share one commit or rollback.
// This composes repositories directly; the production opening use case comes later.
func TestFinancialRepositories(t *testing.T) {
	store, other := repositoryStores(t, "000002_wager_transactions.up.sql", "000003_wallet_ledger_entries.up.sql", "000004_outbox_events.up.sql")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	at := time.Date(2026, 10, 14, 12, 0, 0, 123456789, time.UTC)
	amount, _ := domain.NewMoney(10000, "BRL")
	zero, _ := domain.ZeroMoney("BRL")
	marker := errors.New("injected failure after all inserts")
	// Each case has independent identities; collisions are deliberately introduced after financial writes.
	for _, mode := range []string{"commit", "rollback", "duplicate-ledger", "duplicate-event", "invalid-event"} {
		t.Run(mode, func(t *testing.T) {
			wallet, err := domain.NewWallet(mode, "player-"+mode, amount, at)
			if err != nil {
				t.Fatal(err)
			}
			tx, err := domain.NewOpeningTransaction("tx-"+mode, mode, "player-"+mode, amount, at)
			if err != nil {
				t.Fatal(err)
			}
			tx, err = tx.MarkProcessed(amount, at)
			if err != nil {
				t.Fatal(err)
			}
			entry, err := domain.NewWalletLedgerEntry("entry-"+mode, tx, domain.LedgerCredit, zero, amount, at)
			if err != nil {
				t.Fatal(err)
			}
			processed, err := domain.NewWagerTransactionProcessed(domain.EventMetadata{EventID: "processed-" + mode, CorrelationID: "correlation"}, tx)
			if err != nil {
				t.Fatal(err)
			}
			changed, err := domain.NewWalletBalanceChanged(domain.EventMetadata{EventID: "changed-" + mode, CorrelationID: "correlation", CausationID: "command"}, tx, entry, wallet)
			if err != nil {
				t.Fatal(err)
			}
			err = store.WithinTransaction(ctx, func(r application.Repositories) error {
				if e := r.Wallets.Create(ctx, wallet); e != nil {
					return e
				}
				inserted, e := r.Wagers.Create(ctx, tx)
				if e != nil {
					return e
				}
				if !inserted {
					return errors.New("unexpected transaction collision")
				}
				if e = r.Ledger.Append(ctx, entry); e != nil {
					return e
				}
				if e = r.Outbox.Append(ctx, processed); e != nil {
					return e
				}
				if e = r.Outbox.Append(ctx, changed); e != nil {
					return e
				}
				// Another connection must not see uncommitted state from any adapter.
				if e = other.WithinTransaction(ctx, func(reader application.Repositories) error {
					if _, e := reader.Wallets.Get(ctx, mode); !errors.Is(e, application.ErrWalletNotFound) {
						return errors.New("uncommitted wallet visible")
					}
					if _, e := reader.Wagers.Get(ctx, "tx-"+mode); !errors.Is(e, application.ErrWagerNotFound) {
						return errors.New("uncommitted wager visible")
					}
					if _, e := reader.Ledger.GetByTransaction(ctx, mode, "tx-"+mode); !errors.Is(e, application.ErrLedgerNotFound) {
						return errors.New("uncommitted ledger visible")
					}
					if _, e := reader.Outbox.Get(ctx, "processed-"+mode); !errors.Is(e, application.ErrOutboxNotFound) {
						return errors.New("uncommitted event visible")
					}
					return nil
				}); e != nil {
					return e
				}
				switch mode {
				case "rollback":
					return marker
				case "duplicate-ledger":
					s, _ := entry.Snapshot()
					s.ID = "another-entry"
					duplicate, e := domain.RehydrateWalletLedgerEntry(s)
					if e != nil {
						return e
					}
					return r.Ledger.Append(ctx, duplicate)
				case "duplicate-event":
					return r.Outbox.Append(ctx, processed)
				case "invalid-event":
					return r.Outbox.Append(ctx, domain.WalletBalanceChanged{})
				}
				return nil
			})
			expected := map[string]error{"rollback": marker, "duplicate-ledger": application.ErrLedgerAlreadyExists, "duplicate-event": application.ErrOutboxAlreadyExists, "invalid-event": domain.ErrInvalidEvent}[mode]
			if !errors.Is(err, expected) {
				t.Fatalf("got %v, expected %v", err, expected)
			}
			err = other.WithinTransaction(ctx, func(r application.Repositories) error {
				got, werr := r.Wallets.Get(ctx, mode)
				_, terr := r.Wagers.Get(ctx, "tx-"+mode)
				record, lerr := r.Ledger.GetByTransaction(ctx, mode, "tx-"+mode)
				event, eerr := r.Outbox.Get(ctx, "changed-"+mode)
				original, perr := r.Outbox.Get(ctx, "processed-"+mode)
				if mode != "commit" {
					if !errors.Is(werr, application.ErrWalletNotFound) || !errors.Is(terr, application.ErrWagerNotFound) || !errors.Is(lerr, application.ErrLedgerNotFound) || !errors.Is(eerr, application.ErrOutboxNotFound) || !errors.Is(perr, application.ErrOutboxNotFound) {
						return errors.New("partial financial commit")
					}
					return nil
				}
				if e := errors.Join(werr, terr, lerr, eerr, perr); e != nil {
					return e
				}
				ws, _ := got.Snapshot()
				ls, _ := record.Snapshot()
				if ws.Balance != amount || ls.Money != amount || ls.BalanceBefore != zero || ls.BalanceAfter != amount || ls.Direction != domain.LedgerCredit {
					t.Error("financial mapping changed")
				}
				if event.EventType != "WalletBalanceChanged" || event.AggregateID != mode || event.Version != 1 || event.CausationID != "command" || original.CausationID != "" || original.EventType != "WagerTransactionProcessed" {
					t.Error("event metadata changed")
				}
				if !event.OccurredAt.Equal(at.Truncate(time.Microsecond)) {
					t.Error("event timestamp changed")
				}
				var payload struct {
					Money struct {
						Amount   string `json:"amount"`
						Currency string `json:"currency"`
					} `json:"money"`
					WalletVersion int64 `json:"walletVersion"`
				}
				if e := json.Unmarshal([]byte(event.Payload), &payload); e != nil {
					return e
				}
				if payload.Money.Amount != "100.00" || payload.Money.Currency != "BRL" || payload.WalletVersion != 1 {
					t.Error("event data changed")
				}
				if _, e := r.Ledger.GetByTransaction(ctx, "wrong-wallet", "tx-"+mode); !errors.Is(e, application.ErrLedgerNotFound) {
					t.Error("ledger wallet scope ignored")
				}
				event.Payload = "{}"
				event.EventID = "mutated"
				again, e := r.Outbox.Get(ctx, "changed-"+mode)
				if e != nil {
					return e
				}
				if again.Payload == "{}" {
					t.Error("snapshot mutation persisted")
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
	// Nil and zero-value inputs return errors without issuing invalid writes.
	err := store.WithinTransaction(ctx, func(r application.Repositories) error {
		var pointer *domain.WalletBalanceChanged
		for _, event := range []application.OutboxEvent{nil, pointer, domain.WagerTransactionProcessed{}} {
			if e := r.Outbox.Append(ctx, event); !errors.Is(e, domain.ErrInvalidEvent) {
				return errors.New("invalid event accepted")
			}
		}
		if e := r.Ledger.Append(ctx, domain.WalletLedgerEntry{}); e == nil {
			return errors.New("empty ledger accepted")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
