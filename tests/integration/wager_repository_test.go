//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/application"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

// TestWagerRepository verifies durable identities, original results and shared rollback.
func TestWagerRepository(t *testing.T) {
	store, other := repositoryStores(t, "000002_wager_transactions.up.sql")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	at := time.Date(2026, 10, 14, 11, 0, 0, 123456789, time.UTC)
	amount, _ := domain.NewMoney(8000, "BRL")
	result, _ := domain.NewMoney(2000, "BRL")
	request := domain.WagerRequest{ProviderID: "a", ExternalTransactionID: "ext", IdempotencyKey: "key", WalletID: "w", PlayerID: "player-w", RoundID: "round", GameID: "game", Kind: domain.WagerBet, Money: amount}
	tx, err := domain.NewWagerTransaction("tx", request, at)
	if err != nil {
		t.Fatal(err)
	}
	w := repositoryWallet(t, "w")
	err = store.WithinTransaction(ctx, func(r application.Repositories) error {
		if e := r.Wallets.Create(ctx, w); e != nil {
			return e
		}
		inserted, e := r.Wagers.Create(ctx, tx)
		if e != nil {
			return e
		}
		if !inserted {
			t.Error("first insert collided")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var loaded domain.WagerTransaction
	err = other.WithinTransaction(ctx, func(r application.Repositories) error {
		var e error
		loaded, e = r.Wagers.GetByIdempotencyKey(ctx, "a", "key")
		if e != nil {
			return e
		}
		s, _ := loaded.Snapshot()
		if s.HasResult || s.PayloadHash == "" {
			t.Error("invalid pending snapshot")
		}
		if _, e = r.Wagers.GetByExternalID(ctx, "b", "ext"); !errors.Is(e, application.ErrWagerNotFound) {
			t.Errorf("provider isolation: %v", e)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	processed, err := tx.MarkProcessed(result, at.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	err = store.WithinTransaction(ctx, func(r application.Repositories) error { return r.Wagers.Update(ctx, tx, processed) })
	if err != nil {
		t.Fatal(err)
	}
	err = other.WithinTransaction(ctx, func(r application.Repositories) error { return r.Wagers.Update(ctx, loaded, processed) })
	if !errors.Is(err, application.ErrWagerConflict) {
		t.Fatalf("stale write: %v", err)
	}
	// Both uniqueness constraints reject duplicates while permitting reads in the same transaction.
	for _, mode := range []string{"same", "new-key", "new-external", "changed-payload", "other-provider"} {
		req := request
		switch mode {
		case "new-key":
			req.IdempotencyKey = "new-key"
		case "new-external":
			req.ExternalTransactionID = "new-external"
		case "changed-payload":
			req.GameID = "different"
		case "other-provider":
			req.ProviderID = "b"
		}
		candidate, e := domain.NewWagerTransaction(mode, req, at)
		if e != nil {
			t.Fatal(e)
		}
		err = store.WithinTransaction(ctx, func(r application.Repositories) error {
			inserted, e := r.Wagers.Create(ctx, candidate)
			if e != nil {
				return e
			}
			if inserted != (mode == "other-provider") {
				t.Errorf("unexpected insertion for %s", mode)
			}
			original, e := r.Wagers.GetByExternalID(ctx, "a", "ext")
			if e != nil {
				return e
			}
			s, _ := original.Snapshot()
			if s.ResultBalance != result || s.Status != domain.WagerProcessed {
				t.Error("original result changed")
			}
			e = original.CheckPayload(req)
			if mode == "same" || mode == "new-key" {
				if e != nil {
					t.Error(e)
				}
			} else if !errors.Is(e, domain.ErrWagerPayloadConflict) {
				t.Errorf("payload conflict: %v", e)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// Later wallet changes must not alter the balance returned by a replay lookup.
	err = store.WithinTransaction(ctx, func(r application.Repositories) error {
		before, e := r.Wallets.GetForUpdate(ctx, "w")
		if e != nil {
			return e
		}
		after, e := before.Credit(amount, at)
		if e != nil {
			return e
		}
		return r.Wallets.Update(ctx, before, after)
	})
	if err != nil {
		t.Fatal(err)
	}
	err = other.WithinTransaction(ctx, func(r application.Repositories) error {
		original, e := r.Wagers.GetByIdempotencyKey(ctx, "a", "key")
		if e != nil {
			return e
		}
		s, _ := original.Snapshot()
		if s.ResultBalance != result {
			t.Error("replay used current wallet balance")
		}
		if e = r.Wagers.Update(ctx, original, original); !errors.Is(e, domain.ErrWagerTerminal) {
			t.Errorf("terminal protection: %v", e)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// INTERNAL opening uses SQL NULL metadata and preserves a valid result.
	opening, e := domain.NewOpeningTransaction("opening", "w", "player-w", amount, at)
	if e != nil {
		t.Fatal(e)
	}
	opening, e = opening.MarkProcessed(amount, at)
	if e != nil {
		t.Fatal(e)
	}
	err = store.WithinTransaction(ctx, func(r application.Repositories) error {
		if _, e := r.Wagers.Create(ctx, opening); e != nil {
			return e
		}
		got, e := r.Wagers.Get(ctx, "opening")
		if e != nil {
			return e
		}
		s, _ := got.Snapshot()
		if s.Origin != domain.WagerInternal || s.Request.ProviderID != "" || s.ResultBalance != amount {
			t.Error("invalid opening mapping")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// Rejected/failed results distinguish NULL from a real zero balance.
	for _, zeroResult := range []bool{false, true} {
		req := request
		id := "absent"
		if zeroResult {
			id = "zero"
		}
		req.ExternalTransactionID = id
		req.IdempotencyKey = id
		candidate, e := domain.NewWagerTransaction(id, req, at)
		if e != nil {
			t.Fatal(e)
		}
		if zeroResult {
			zero, _ := domain.ZeroMoney("BRL")
			candidate, e = candidate.Fail(&zero, at)
		} else {
			candidate, e = candidate.Reject(domain.FailureInsufficientFunds, nil, at)
		}
		if e != nil {
			t.Fatal(e)
		}
		err = store.WithinTransaction(ctx, func(r application.Repositories) error {
			if _, e := r.Wagers.Create(ctx, candidate); e != nil {
				return e
			}
			got, e := r.Wagers.Get(ctx, id)
			if e != nil {
				return e
			}
			s, _ := got.Snapshot()
			if s.HasResult != zeroResult {
				t.Error("NULL and zero conflated")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	marker := errors.New("rollback both repositories")
	err = store.WithinTransaction(ctx, func(r application.Repositories) error {
		before, e := r.Wallets.Get(ctx, "w")
		if e != nil {
			return e
		}
		after, e := before.Credit(amount, at)
		if e != nil {
			return e
		}
		if e = r.Wallets.Update(ctx, before, after); e != nil {
			return e
		}
		req := request
		req.ExternalTransactionID = "rolled"
		req.IdempotencyKey = "rolled"
		candidate, e := domain.NewWagerTransaction("rolled", req, at)
		if e != nil {
			return e
		}
		if _, e = r.Wagers.Create(ctx, candidate); e != nil {
			return e
		}
		return marker
	})
	if !errors.Is(err, marker) {
		t.Fatal(err)
	}
	err = other.WithinTransaction(ctx, func(r application.Repositories) error {
		if _, e := r.Wagers.Get(ctx, "rolled"); !errors.Is(e, application.ErrWagerNotFound) {
			t.Error("wager survived rollback")
		}
		wallet, e := r.Wallets.Get(ctx, "w")
		if e != nil {
			return e
		}
		s, _ := wallet.Snapshot()
		minor, _ := s.Balance.Minor()
		if minor != 18000 || s.Version != 2 {
			t.Error("wallet survived rollback")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
