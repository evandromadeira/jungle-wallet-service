package application

import (
	"bytes"
	"context"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
	"log/slog"
	"strings"
	"testing"
)

type reconciliationFixture struct{ result Reconciliation }

func (f reconciliationFixture) ReadReconciliation(context.Context, string) (Reconciliation, error) {
	return f.result, nil
}

// TestReconciliationDiscrepancy verifies signed differences, counters and safe logs.
func TestReconciliationDiscrepancy(t *testing.T) {
	stored, _ := domain.NewMoney(6500, "BRL")
	calculated, _ := domain.NewMoney(7500, "BRL")
	var logs bytes.Buffer
	service, err := NewReconcileWallet(reconciliationFixture{Reconciliation{WalletID: "wallet", StoredBalance: stored, CalculatedBalance: calculated, CheckedEntries: 3}}, slog.New(slog.NewJSONHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	result, err := service.Execute(context.Background(), "wallet", "correlation")
	if err != nil {
		t.Fatal(err)
	}
	difference, _ := domain.NewMoney(-1000, "BRL")
	if result.Consistent || result.Difference != difference || service.Discrepancies() != 1 {
		t.Fatal("discrepancy not reported")
	}
	if !strings.Contains(logs.String(), "walletId") || !strings.Contains(logs.String(), "correlationId") || strings.Contains(logs.String(), "75.00") {
		t.Fatal("unsafe or incomplete log")
	}
	if result.StoredBalance != stored || result.CalculatedBalance != calculated {
		t.Fatal("audit changed values")
	}
}
