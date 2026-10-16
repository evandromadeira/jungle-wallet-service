//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

// TestWinAndLoss verifies credits, no-movement outcomes, references and overflow on PostgreSQL.
func TestWinAndLoss(t *testing.T) {
	store, reader := repositoryStores(t, "000002_wager_transactions.up.sql", "000003_wallet_ledger_entries.up.sql", "000004_outbox_events.up.sql")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	initial, _ := domain.NewMoney(10000, "BRL")
	wallet, err := openingService(t, store, "open-win").Execute(ctx, application.OpenWalletCommand{PlayerID: "p", InitialBalance: initial, CorrelationID: "c"})
	if err != nil {
		t.Fatal(err)
	}
	ws, _ := wallet.Snapshot()
	n := 0
	processor, err := application.NewProcessWager(store, func() (string, error) { n++; return fmt.Sprintf("wager-%d", n), nil }, func() time.Time { return time.Date(2026, 10, 14, 14, 0, 0, 0, time.UTC) })
	if err != nil {
		t.Fatal(err)
	}
	run := func(id string, kind domain.WagerKind, minor int64, reference string) application.WagerResult {
		t.Helper()
		money, _ := domain.NewMoney(minor, "BRL")
		c := application.WagerCommand{AuthorizedProviderID: "a", CorrelationID: "c", Request: domain.WagerRequest{ProviderID: "a", ExternalTransactionID: id, IdempotencyKey: id, WalletID: ws.ID, PlayerID: "p", RoundID: "r", GameID: "g", Kind: kind, Money: money, ReferenceExternalTransactionID: reference}}
		got, e := processor.Execute(ctx, c)
		if e != nil {
			t.Fatal(e)
		}
		return got
	}
	bet := run("bet", domain.WagerBet, 1000, "")
	bs, _ := bet.Transaction.Snapshot()
	win := run("win", domain.WagerWin, 2000, "bet")
	winState, _ := win.Transaction.Snapshot()
	if winState.Status != domain.WagerProcessed || winState.ReferenceTransactionID != bs.ID {
		t.Fatal("referenced WIN failed")
	}
	freeWin := run("free", domain.WagerWin, 100, "")
	fs, _ := freeWin.Transaction.Snapshot()
	if fs.Status != domain.WagerProcessed {
		t.Fatal("unreferenced WIN failed")
	}
	countBeforeLoss := n
	loss := run("loss", domain.WagerLoss, 0, "")
	ls, _ := loss.Transaction.Snapshot()
	if ls.Status != domain.WagerProcessed || n-countBeforeLoss != 2 {
		t.Fatal("LOSS should allocate only transaction and processed event")
	}
	err = reader.WithinTransaction(ctx, func(r application.Repositories) error {
		w, e := r.Wallets.Get(ctx, ws.ID)
		if e != nil {
			return e
		}
		s, _ := w.Snapshot()
		minor, _ := s.Balance.Minor()
		if minor != 11100 || s.Version != 4 {
			t.Error("LOSS changed wallet")
		}
		entry, e := r.Ledger.GetByTransaction(ctx, ws.ID, winState.ID)
		if e != nil {
			return e
		}
		es, _ := entry.Snapshot()
		if es.Direction != domain.LedgerCredit {
			t.Error("WIN not credited")
		}
		if _, e = r.Ledger.GetByTransaction(ctx, ws.ID, ls.ID); !errors.Is(e, application.ErrLedgerNotFound) {
			t.Error("LOSS created ledger")
		}
		event, e := r.Outbox.Get(ctx, fmt.Sprintf("wager-%d", countBeforeLoss+2))
		if e != nil {
			return e
		}
		if event.EventType != "WagerTransactionProcessed" {
			t.Error("LOSS event wrong")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		id    string
		kind  domain.WagerKind
		minor int64
		ref   string
	}{{"win", domain.WagerWin, 2000, "bet"}, {"loss", domain.WagerLoss, 0, ""}} {
		replay := run(c.id, c.kind, c.minor, c.ref)
		if !replay.IdempotentReplay {
			t.Fatal("replay reapplied operation")
		}
	}
	waiting := run("waiting", domain.WagerWin, 100, "missing")
	waitState, _ := waiting.Transaction.Snapshot()
	if waitState.Status != domain.WagerPendingReference || waitState.HasResult {
		t.Fatal("missing reference not pending")
	}
	if !run("waiting", domain.WagerWin, 100, "missing").IdempotentReplay {
		t.Fatal("pending replay created new operation")
	}
	wrong := run("wrong-ref", domain.WagerWin, 100, "free")
	wrongState, _ := wrong.Transaction.Snapshot()
	if wrongState.FailureCode != domain.FailureReferenceMismatch {
		t.Fatal("wrong reference kind accepted")
	}
	rejectedBet := run("too-large", domain.WagerBet, 20000, "")
	rs, _ := rejectedBet.Transaction.Snapshot()
	if rs.Status != domain.WagerRejected {
		t.Fatal("fixture not rejected")
	}
	rejectedWin := run("bad-ref", domain.WagerWin, 100, "too-large")
	rws, _ := rejectedWin.Transaction.Snapshot()
	if rws.FailureCode != domain.FailureReferenceUnsuccessful {
		t.Fatal("unsuccessful reference accepted")
	}
	// Pending BET reference is a persisted fixture for asynchronous/recovery scenarios.
	err = store.WithinTransaction(ctx, func(r application.Repositories) error {
		money, _ := domain.NewMoney(100, "BRL")
		tx, e := domain.NewWagerTransaction("pending-bet", domain.WagerRequest{ProviderID: "a", ExternalTransactionID: "pending-bet", IdempotencyKey: "pending-bet", WalletID: ws.ID, PlayerID: "p", RoundID: "r", GameID: "g", Kind: domain.WagerBet, Money: money}, time.Date(2026, 10, 14, 13, 0, 0, 0, time.UTC))
		if e != nil {
			return e
		}
		_, e = r.Wagers.Create(ctx, tx)
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	pendingWin := run("waiting-existing", domain.WagerWin, 100, "pending-bet")
	pws, _ := pendingWin.Transaction.Snapshot()
	if pws.Status != domain.WagerPendingReference {
		t.Fatal("pending reference not retained")
	}
	huge := run("overflow", domain.WagerWin, math.MaxInt64, "")
	hs, _ := huge.Transaction.Snapshot()
	if hs.Status != domain.WagerRejected || hs.FailureCode != domain.FailureBalanceOverflow {
		t.Fatal("overflow not audited")
	}
	err = reader.WithinTransaction(ctx, func(r application.Repositories) error {
		for _, s := range []domain.WagerState{waitState, wrongState, rws, pws, hs} {
			if _, e := r.Ledger.GetByTransaction(ctx, ws.ID, s.ID); !errors.Is(e, application.ErrLedgerNotFound) {
				t.Error("nonprocessed WIN changed ledger")
			}
		}
		w, e := r.Wallets.Get(ctx, ws.ID)
		if e != nil {
			return e
		}
		s, _ := w.Snapshot()
		minor, _ := s.Balance.Minor()
		if minor != 11100 || s.Version != 4 {
			t.Error("replay/rejection moved funds")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
