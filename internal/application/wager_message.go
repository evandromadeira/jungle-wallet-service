package application

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/evandromadeira/jungle-wallet-service/internal/domain"
)

// WagerMessage is the typed incoming envelope; messageId is not an SQS receipt handle.
type WagerMessage struct {
	MessageID  string    `json:"messageId"`
	Type       string    `json:"type"`
	OccurredAt time.Time `json:"occurredAt"`
	Data       struct {
		ProviderID                     string           `json:"providerId"`
		ExternalTransactionID          string           `json:"externalTransactionId"`
		IdempotencyKey                 string           `json:"idempotencyKey"`
		WalletID                       string           `json:"walletId"`
		PlayerID                       string           `json:"playerId"`
		RoundID                        string           `json:"roundId"`
		GameID                         string           `json:"gameId"`
		Kind                           domain.WagerKind `json:"kind"`
		Money                          domain.Money     `json:"money"`
		ReferenceExternalTransactionID string           `json:"referenceExternalTransactionId,omitempty"`
	} `json:"data"`
}

// HandleMessage parses and hashes the same byte snapshot, preventing mismatched command/hash pairs.
// Consumer, authorizedProvider and correlationID come from trusted transport configuration.
// Only a nil error permits the future SQS adapter to delete its receipt after commit.
func (u *ProcessWager) HandleMessage(ctx context.Context, consumer, authorizedProvider, correlationID string, body []byte) (observed WagerResult, observedErr error) {
	start := time.Now()
	var message WagerMessage
	defer func() {
		observeWager(ctx, "sqs_wager", start, observed, observedErr, correlationID, message.MessageID, authorizedProvider, message.Data.WalletID)
	}()
	if err := ctx.Err(); err != nil {
		return WagerResult{}, err
	}
	if consumer == "" || strings.TrimSpace(consumer) != consumer {
		return WagerResult{}, ErrInvalidInboxMessage
	}
	body = bytes.Clone(body)

	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&message); err != nil {
		return WagerResult{}, ErrInvalidInboxMessage
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return WagerResult{}, ErrInvalidInboxMessage
	}
	if message.MessageID == "" || strings.TrimSpace(message.MessageID) != message.MessageID || message.Type != "WagerTransactionRequested" || message.OccurredAt.IsZero() {
		return WagerResult{}, ErrInvalidInboxMessage
	}
	d := message.Data
	command := WagerCommand{AuthorizedProviderID: authorizedProvider, CorrelationID: correlationID, CausationID: message.MessageID,
		Request: domain.WagerRequest{ProviderID: d.ProviderID, ExternalTransactionID: d.ExternalTransactionID, IdempotencyKey: d.IdempotencyKey, WalletID: d.WalletID, PlayerID: d.PlayerID, RoundID: d.RoundID, GameID: d.GameID, Kind: d.Kind, Money: d.Money, ReferenceExternalTransactionID: d.ReferenceExternalTransactionID}}
	pending, err := u.prepare(ctx, command)
	if err != nil {
		return WagerResult{}, err
	}
	digest := sha256.Sum256(body)
	hash := hex.EncodeToString(digest[:])
	received := u.now().Round(0).UTC().Truncate(time.Microsecond)
	var result WagerResult
	err = u.work.WithinTransaction(ctx, func(r Repositories) error {
		if e := r.Inbox.Receive(ctx, InboxMessage{ConsumerName: consumer, MessageID: message.MessageID, PayloadHash: hash, ReceivedAt: received}); e != nil {
			return e
		}
		receipt, e := r.Inbox.GetForUpdate(ctx, consumer, message.MessageID)
		if e != nil {
			return e
		}
		if receipt.PayloadHash != hash {
			return ErrInboxPayloadConflict
		}
		if receipt.CompletedAt != nil {
			existing, found, e := findWagerReplay(ctx, r.Wagers, command.Request)
			if e != nil {
				return e
			}
			if !found {
				return ErrInboxOutcomeMissing
			}
			result = WagerResult{existing, true}
			return nil
		}
		result, e = u.processWithin(ctx, r, command, pending)
		if e != nil {
			return e
		}
		completed := u.now().Round(0).UTC().Truncate(time.Microsecond)
		if completed.Before(receipt.ReceivedAt) {
			completed = receipt.ReceivedAt
		}
		return r.Inbox.Complete(ctx, consumer, message.MessageID, completed)
	})
	if err != nil {
		return WagerResult{}, err
	}
	return result, nil
}
