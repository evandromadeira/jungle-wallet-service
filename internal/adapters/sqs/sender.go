package sqs

import (
	"context"
	"errors"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/evandromadeira/jungle-wallet-service/internal/application"
)

// Sender delivers the unchanged outbox envelope to one configured standard queue.
// The client owns endpoint, credentials and bounded HTTP retry configuration.
type Sender struct {
	client   *awssqs.Client
	queueURL string
}

var _ application.EventSender = (*Sender)(nil)

// NewSender validates a standard queue destination without making a network request.
func NewSender(client *awssqs.Client, queueURL string) (*Sender, error) {
	u, err := url.Parse(queueURL)
	if client == nil || err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.Trim(u.Path, "/") == "" || strings.HasSuffix(u.Path, ".fifo") {
		return nil, errors.New("invalid standard SQS queue configuration")
	}
	return &Sender{client, queueURL}, nil
}

// Send returns success only after SQS accepts the message. The publisher owns retries.
// Transport errors are deliberately redacted; they may contain request details.
func (s *Sender) Send(ctx context.Context, body []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(body) == 0 {
		return errors.New("empty SQS event")
	}
	out, err := s.client.SendMessage(ctx, &awssqs.SendMessageInput{QueueUrl: aws.String(s.queueURL), MessageBody: aws.String(string(body))})
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return errors.New("SQS send failed")
	}
	if out == nil || aws.ToString(out.MessageId) == "" {
		return errors.New("SQS acceptance missing message ID")
	}
	return nil
}
