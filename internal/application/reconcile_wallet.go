package application

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"

	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

// Reconciliation is a read-only comparison from one database statement snapshot.
type Reconciliation struct {
	WalletID          string
	StoredBalance     domain.Money
	CalculatedBalance domain.Money
	Difference        domain.Money
	Consistent        bool
	CheckedEntries    int64
}

// ReconciliationReader reconstructs all credits minus debits, including OPENING.
type ReconciliationReader interface {
	ReadReconciliation(context.Context, string) (Reconciliation, error)
}

// ReconcileWallet records discrepancy observations without repairing financial data.
type ReconcileWallet struct {
	reader        ReconciliationReader
	log           *slog.Logger
	discrepancies atomic.Uint64
}

// NewReconcileWallet requires an explicit reader and structured logger.
func NewReconcileWallet(reader ReconciliationReader, logger *slog.Logger) (*ReconcileWallet, error) {
	if reader == nil || logger == nil {
		return nil, errors.New("reconciliation dependencies required")
	}
	return &ReconcileWallet{reader: reader, log: logger}, nil
}

// Execute compares balances and reports discrepancies without logging monetary payloads.
func (s *ReconcileWallet) Execute(ctx context.Context, id, correlation string) (Reconciliation, error) {
	if id == "" || strings.TrimSpace(id) != id {
		return Reconciliation{}, ErrInvalidWalletQuery
	}
	result, err := s.reader.ReadReconciliation(ctx, id)
	if err != nil {
		return Reconciliation{}, err
	}
	result.Difference, err = result.StoredBalance.Sub(result.CalculatedBalance)
	if err != nil {
		return Reconciliation{}, err
	}
	result.Consistent, err = result.Difference.IsZero()
	if err != nil {
		return Reconciliation{}, err
	}
	if !result.Consistent {
		s.discrepancies.Add(1)
		s.log.WarnContext(ctx, "wallet reconciliation discrepancy", "walletId", id, "correlationId", correlation)
	}
	return result, nil
}

// Discrepancies counts divergent checks in this process, including repeated checks.
func (s *ReconcileWallet) Discrepancies() uint64 { return s.discrepancies.Load() }
