package application

import (
	"context"
	"errors"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

var (
	ErrWalletNotFound      = errors.New("wallet not found")
	ErrWalletAlreadyExists = errors.New("wallet already exists")
	ErrWalletConflict      = errors.New("wallet changed since it was read")
	ErrInvalidWalletUpdate = errors.New("invalid wallet state progression")
)

// WalletRepository belongs to one transaction; callers must not retain it or use it concurrently.
type WalletRepository interface {
	Create(context.Context, domain.Wallet) error
	Get(context.Context, string) (domain.Wallet, error)
	GetForUpdate(context.Context, string) (domain.Wallet, error)
	Update(context.Context, domain.Wallet, domain.Wallet) error
}

// Repositories groups adapters sharing the same database transaction.
type Repositories struct {
	Wallets WalletRepository
	Wagers  WagerRepository
	Ledger  LedgerRepository
	Outbox  OutboxRepository
	Inbox   InboxRepository
}

// UnitOfWork commits only when the callback succeeds. Callbacks must propagate errors
// Financial callbacks must not publish external effects. Only the dedicated outbox worker
// sends previously committed events. Commit errors must not be blindly retried.
type UnitOfWork interface {
	WithinTransaction(context.Context, func(Repositories) error) error
}

var (
	ErrWagerNotFound      = errors.New("wager transaction not found")
	ErrWagerConflict      = errors.New("wager transaction changed since it was read")
	ErrInvalidWagerUpdate = errors.New("immutable wager fields changed")
)

// WagerRepository belongs to the same transaction as WalletRepository.
// Get is internal; provider-facing reads must use the provider-scoped methods.
// Create returns false on an identity collision without aborting the SQL transaction.
// The caller must then inspect BOTH external identities and compare business content.
type WagerRepository interface {
	DueReferences(context.Context, time.Time, int) ([]ReferenceWork, error)
	ReferenceWork(context.Context, string) (ReferenceWork, error)
	ScheduleReference(context.Context, string, int, time.Time) error
	// HasSuccessfulReversal must be called while holding the original wallet lock.
	HasSuccessfulReversal(context.Context, string) (bool, error)
	Create(context.Context, domain.WagerTransaction) (bool, error)
	Get(context.Context, string) (domain.WagerTransaction, error)
	GetByExternalID(context.Context, string, string) (domain.WagerTransaction, error)
	GetByIdempotencyKey(context.Context, string, string) (domain.WagerTransaction, error)
	Update(context.Context, domain.WagerTransaction, domain.WagerTransaction) error
}

var (
	ErrLedgerNotFound      = errors.New("ledger entry not found")
	ErrLedgerAlreadyExists = errors.New("ledger entry already exists")
	ErrOutboxNotFound      = errors.New("outbox event not found")
	ErrOutboxAlreadyExists = errors.New("outbox event already exists")
)

// LedgerRepository appends financial records; mutation and deletion are not exposed.
type LedgerRepository interface {
	Append(context.Context, domain.WalletLedgerEntry) error
	GetByTransaction(context.Context, string, string) (domain.WalletLedgerEntry, error)
}

// OutboxEvent exposes the snapshot supplied by the four concrete domain event values.
// Adapters reject other implementations and pointers, including typed nil pointers.
type OutboxEvent interface {
	Snapshot() (domain.EventSnapshot, error)
}

// OutboxRepository stores events and coordinates delivery in a separate transaction.
type OutboxRepository interface {
	// ClaimDue locks one committed event until transaction completion; skips other workers.
	ClaimDue(context.Context, time.Time) (OutboxDelivery, error)
	// FinishDelivery requires the ClaimDue lock. Failure stores a fixed, non-sensitive error.
	FinishDelivery(context.Context, string, bool, time.Time) error
	Append(context.Context, OutboxEvent) error
	Get(context.Context, string) (domain.EventSnapshot, error)
}

// ReferenceWork carries durable scheduling and tracing from the initial outbox event.
type ReferenceWork struct {
	TransactionID string
	WalletID      string
	Attempts      int
	NextAttemptAt time.Time
	CorrelationID string
	CausationID   string
}

var (
	ErrInboxNotFound        = errors.New("inbox message not found")
	ErrInboxPayloadConflict = errors.New("message identity reused with different content")
	ErrInvalidInboxMessage  = errors.New("invalid inbox message")
	ErrInboxOutcomeMissing  = errors.New("completed inbox has no durable wager outcome")
)

// InboxMessage is a detached receipt; CompletedAt nil means no durable completion.
type InboxMessage struct {
	ConsumerName string
	MessageID    string
	PayloadHash  string
	ReceivedAt   time.Time
	CompletedAt  *time.Time
}

// InboxRepository locks a receipt before financial locks. Complete never commits alone.
type InboxRepository interface {
	Receive(context.Context, InboxMessage) error
	GetForUpdate(context.Context, string, string) (InboxMessage, error)
	Complete(context.Context, string, string, time.Time) error
}
