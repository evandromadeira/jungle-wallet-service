package application

import (
	"context"
	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
	"time"
)

// Compatibility names preserve the existing BET-only contract.
type BetCommand = WagerCommand
type BetResult = WagerResult

var ErrInvalidBet = ErrInvalidWagerCommand
var ErrBetWalletMismatch = ErrWagerWalletMismatch
var ErrBetPending = ErrWagerPending

type ProcessBet struct{ processor *ProcessWager }

// NewProcessBet wraps the shared processor for BET-only callers.
func NewProcessBet(work UnitOfWork, newID func() (string, error), now func() time.Time) (*ProcessBet, error) {
	p, err := NewProcessWager(work, newID, now)
	if err != nil {
		return nil, err
	}
	return &ProcessBet{p}, nil
}

// Execute rejects other kinds before delegating to shared processing.
func (u *ProcessBet) Execute(ctx context.Context, c BetCommand) (BetResult, error) {
	if c.Request.Kind != domain.WagerBet {
		return BetResult{}, ErrInvalidBet
	}
	return u.processor.Execute(ctx, c)
}
